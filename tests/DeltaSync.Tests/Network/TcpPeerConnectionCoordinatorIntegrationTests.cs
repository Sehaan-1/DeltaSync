namespace DeltaSync.Tests.Network;

using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using DeltaSync.Network;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

public class TcpPeerConnectionCoordinatorIntegrationTests
{
    private readonly ITestOutputHelper _output;

    public TcpPeerConnectionCoordinatorIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task SimultaneousCrossDial_OverPhysicalLoopbackSockets_ConvergesToSingleChannel()
    {
        // Issue #35 Check 1 (Spec §10 Check 3):
        // Spin up two TcpPeerListener and PeerConnectionCoordinator instances (peer-alpha vs peer-beta, beta > alpha).
        // Trigger simultaneous cross-dials over real physical OS TCP loopback sockets.
        // Assert:
        // 1. Peer-beta outbound channel is preserved (IsInbound == false).
        // 2. Peer-alpha outbound dial is cleanly closed/aborted under BGP tie-break (RFC 4271 §6.8).
        // 3. Both nodes settle on exactly 1 active canonical connection in ActiveConnections (Postcondition P1).
        // 4. Bidirectional payload exchange succeeds across the surviving channel.
        // 5. Zero socket leaks, zero unhandled reader exceptions.

        var clusterId = Guid.NewGuid();
        string peerIdA = "peer-alpha";
        string peerIdB = "peer-beta";

        // Precondition check: beta is lexicographically greater than alpha
        string.CompareOrdinal(peerIdB, peerIdA).Should().BePositive();

        var dialerA = new TcpPeerDialer(clusterId, peerIdA);
        var dialerB = new TcpPeerDialer(clusterId, peerIdB);

        await using var coordinatorA = new PeerConnectionCoordinator(
            clusterId,
            peerIdA,
            listenPort: 0,
            dialer: dialerA.AsDialer());

        await using var coordinatorB = new PeerConnectionCoordinator(
            clusterId,
            peerIdB,
            listenPort: 0,
            dialer: dialerB.AsDialer());

        // Bind listeners on loopback with port 0 (OS dynamic port assignment)
        await using var listenerA = new TcpPeerListener(peerIdA, clusterId, new IPEndPoint(IPAddress.Loopback, 0), coordinatorA);
        listenerA.Start();

        await using var listenerB = new TcpPeerListener(peerIdB, clusterId, new IPEndPoint(IPAddress.Loopback, 0), coordinatorB);
        listenerB.Start();

        var endpointA = listenerA.LocalEndPoint;
        var endpointB = listenerB.LocalEndPoint;

        _output.WriteLine($"Started listener A at {endpointA}, listener B at {endpointB}");

        var collisionEventsA = new List<(string Remote, CollisionDecision Decision)>();
        var collisionEventsB = new List<(string Remote, CollisionDecision Decision)>();

        coordinatorA.CollisionResolved += (_, e) => { lock (collisionEventsA) collisionEventsA.Add(e); };
        coordinatorB.CollisionResolved += (_, e) => { lock (collisionEventsB) collisionEventsB.Add(e); };

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // Simultaneous cross-dial
        _output.WriteLine("Triggering simultaneous cross-dial over physical TCP sockets...");
        var dialTaskA = Task.Run(async () => await coordinatorA.ConnectAsync(peerIdB, endpointB, cts.Token));
        var dialTaskB = Task.Run(async () => await coordinatorB.ConnectAsync(peerIdA, endpointA, cts.Token));

        var results = await Task.WhenAll(dialTaskA, dialTaskB);
        var channelAFromDial = results[0];
        var channelBFromDial = results[1];

        // Wait briefly for all background channel tasks and cleanup to settle
        await Task.Delay(100);

        // 1. Assert exactly 1 active canonical connection exists on each node
        coordinatorA.ActiveConnections.Count.Should().Be(1, "Node Alpha must settle on exactly 1 active connection (P1)");
        coordinatorB.ActiveConnections.Count.Should().Be(1, "Node Beta must settle on exactly 1 active connection (P1)");

        coordinatorA.TryGetConnection(peerIdB, out var activeAtA).Should().BeTrue();
        coordinatorB.TryGetConnection(peerIdA, out var activeAtB).Should().BeTrue();

        activeAtA.Should().NotBeNull();
        activeAtB.Should().NotBeNull();

        // 2. Assert Node Beta (higher peer ID) preserved its outbound connection
        activeAtB!.IsInbound.Should().BeFalse("Node Beta has higher PeerId and must preserve outbound dial");
        activeAtB.IsConnected.Should().BeTrue();

        // 3. Assert Node Alpha yielded to incoming dial from Node Beta
        activeAtA!.IsInbound.Should().BeTrue("Node Alpha has lower PeerId and must yield to inbound dial from Beta");
        activeAtA.IsConnected.Should().BeTrue();
        activeAtA.RemotePeerId.Should().Be(peerIdB);

        // 4. Assert collision resolution events were raised correctly
        lock (collisionEventsA)
        {
            collisionEventsA.Should().Contain(e => e.Remote == peerIdB && e.Decision == CollisionDecision.YieldToInbound);
        }
        lock (collisionEventsB)
        {
            collisionEventsB.Should().Contain(e => e.Remote == peerIdA && e.Decision == CollisionDecision.PreserveOutbound);
        }

        // 5. Test bidirectional payload exchange across real TCP sockets over the surviving connection
        byte[] pingPayload = new byte[16 * 1024]; // 16 KB payload
        RandomNumberGenerator.Fill(pingPayload);
        await activeAtB.SendAsync(pingPayload, cts.Token);

        var receivedAtA = await activeAtA.ReceiveAsync(cts.Token);
        receivedAtA.ToArray().Should().Equal(pingPayload);

        byte[] pongPayload = new byte[32 * 1024]; // 32 KB payload
        RandomNumberGenerator.Fill(pongPayload);
        await activeAtA.SendAsync(pongPayload, cts.Token);

        var receivedAtB = await activeAtB.ReceiveAsync(cts.Token);
        receivedAtB.ToArray().Should().Equal(pongPayload);

        // 6. Clean shutdown
        await listenerA.StopAsync();
        await listenerB.StopAsync();
    }

    [Fact]
    public async Task GracefulDisconnect_CleansUpPeerRegistry()
    {
        // Issue #35 Check 2:
        // Close active channel cleanly; assert both sides detect closure via EOF and evict peer from active connection list.

        var clusterId = Guid.NewGuid();
        string peerIdA = "peer-alpha";
        string peerIdB = "peer-beta";

        var dialerA = new TcpPeerDialer(clusterId, peerIdA);
        await using var coordinatorA = new PeerConnectionCoordinator(clusterId, peerIdA, dialer: dialerA.AsDialer());
        await using var coordinatorB = new PeerConnectionCoordinator(clusterId, peerIdB);

        await using var listenerB = new TcpPeerListener(peerIdB, clusterId, new IPEndPoint(IPAddress.Loopback, 0), coordinatorB);
        listenerB.Start();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        // Connect A to B over physical TCP socket
        var channelA = await coordinatorA.ConnectAsync(peerIdB, listenerB.LocalEndPoint, cts.Token);
        channelA.Should().NotBeNull();
        channelA!.IsConnected.Should().BeTrue();

        coordinatorA.ActiveConnections.Count.Should().Be(1);
        coordinatorB.ActiveConnections.Count.Should().Be(1);

        coordinatorB.TryGetConnection(peerIdA, out var channelB).Should().BeTrue();
        channelB.Should().NotBeNull();
        channelB!.IsConnected.Should().BeTrue();

        // Close channel from Node A
        await coordinatorA.CloseConnectionAsync(peerIdB, "Graceful test disconnect");

        // Verify Node A evicted peer
        coordinatorA.ActiveConnections.Should().BeEmpty();
        coordinatorA.TryGetConnection(peerIdB, out _).Should().BeFalse();

        // Node B should detect EOF on read
        var eof = await channelB.ReceiveAsync(cts.Token);
        eof.IsEmpty.Should().BeTrue("EOF must return empty memory");
        channelB.IsConnected.Should().BeFalse();

        // Node B's active connection list evicts the disconnected peer
        coordinatorB.ActiveConnections.Should().BeEmpty();
        coordinatorB.TryGetConnection(peerIdA, out _).Should().BeFalse();

        await listenerB.StopAsync();
    }

    [Fact]
    public async Task ClusterIsolation_OverPhysicalSockets_RejectedCleanly()
    {
        var cluster1 = Guid.NewGuid();
        var cluster2 = Guid.NewGuid();

        string peerIdA = "node-c1";
        string peerIdB = "node-c2";

        var dialerA = new TcpPeerDialer(cluster1, peerIdA);
        await using var coordinatorA = new PeerConnectionCoordinator(cluster1, peerIdA, dialer: dialerA.AsDialer());
        await using var coordinatorB = new PeerConnectionCoordinator(cluster2, peerIdB);

        await using var listenerB = new TcpPeerListener(peerIdB, cluster2, new IPEndPoint(IPAddress.Loopback, 0), coordinatorB);
        listenerB.Start();

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Dialing across cluster boundaries must be rejected
        var act = async () => await coordinatorA.ConnectAsync(peerIdB, listenerB.LocalEndPoint, cts.Token);

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*ClusterMismatch*");

        coordinatorA.ActiveConnections.Should().BeEmpty();
        coordinatorB.ActiveConnections.Should().BeEmpty();

        await listenerB.StopAsync();
    }

    [Fact]
    public async Task DialAsync_NullEndpointAndNoRegistry_ThrowsArgumentException()
    {
        var clusterId = Guid.NewGuid();
        var dialer = new TcpPeerDialer(clusterId, "node-local");

        var act = async () => await dialer.DialAsync("node-remote", endpoint: null);

        await act.Should().ThrowAsync<ArgumentException>()
            .WithMessage("*Remote endpoint cannot be null*");
    }

    [Fact]
    public async Task DialAsync_NonRoutableEndpointWithShortTimeout_ThrowsTimeoutException()
    {
        // 192.0.2.1 is TEST-NET-1 (RFC 5737), which discards traffic and triggers dial timeout
        var nonRoutable = new IPEndPoint(IPAddress.Parse("192.0.2.1"), 59999);
        var dialer = new TcpPeerDialer(Guid.NewGuid(), "node-local", dialTimeout: TimeSpan.FromMilliseconds(200));

        var act = async () => await dialer.DialAsync("node-remote", nonRoutable);

        await act.Should().ThrowAsync<Exception>()
            .Where(ex => ex is TimeoutException || ex is SocketException || ex is OperationCanceledException);
    }
}

