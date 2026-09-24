namespace DeltaSync.Tests.Network;

using System.Net;
using DeltaSync.Network;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

public class PeerConnectionCoordinatorTests
{
    private readonly ITestOutputHelper _output;

    public PeerConnectionCoordinatorTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task SpecSection10_Check2_ConnectionCollision_SimultaneousCrossDial_ConvergesToOne()
    {
        // Spec §10 Check 2:
        // Instantiate two mock nodes A ("node-alpha") and B ("node-beta") with PeerId_B > PeerId_A.
        // Simulate simultaneous bidirectional dial: both A -> B and B -> A trigger concurrently.
        // Verify through collision resolver:
        // - Node B's outbound connection is preserved.
        // - Node A's outbound connection is aborted and disposed (zero socket leaks).
        // - Exactly one duplex transport channel survives between A and B (Postcondition P1, Invariant I1).

        var clusterId = Guid.NewGuid();
        string peerIdA = "node-alpha";
        string peerIdB = "node-beta";

        // Confirm lexicographical ordering precondition
        string.CompareOrdinal(peerIdB, peerIdA).Should().BePositive("node-beta must be strictly greater than node-alpha");

        PeerConnectionCoordinator? coordinatorA = null;
        PeerConnectionCoordinator? coordinatorB = null;

        InMemoryTransportChannel? alphaOutboundCreated = null;
        InMemoryTransportChannel? betaOutboundCreated = null;

        var collisionDecisionsA = new List<(string Remote, CollisionDecision Decision)>();
        var collisionDecisionsB = new List<(string Remote, CollisionDecision Decision)>();

        // Set up cross-wired dialers
        coordinatorA = new PeerConnectionCoordinator(
            clusterId,
            peerIdA,
            listenPort: 5001,
            dialer: (remoteId, _, _) =>
            {
                var (localOut, remoteIn) = InMemoryTransportChannel.CreateConnectedPair(peerIdA, remoteId, clusterId);
                alphaOutboundCreated = localOut;
                _ = Task.Run(async () =>
                {
                    await Task.Yield();
                    await coordinatorB!.AcceptConnectionAsync(remoteIn);
                });
                return ValueTask.FromResult<IPeerTransportChannel>(localOut);
            });

        coordinatorB = new PeerConnectionCoordinator(
            clusterId,
            peerIdB,
            listenPort: 5002,
            dialer: (remoteId, _, _) =>
            {
                var (localOut, remoteIn) = InMemoryTransportChannel.CreateConnectedPair(peerIdB, remoteId, clusterId);
                betaOutboundCreated = localOut;
                _ = Task.Run(async () =>
                {
                    await Task.Yield();
                    await coordinatorA!.AcceptConnectionAsync(remoteIn);
                });
                return ValueTask.FromResult<IPeerTransportChannel>(localOut);
            });

        coordinatorA.CollisionResolved += (_, e) =>
        {
            lock (collisionDecisionsA) collisionDecisionsA.Add(e);
        };

        coordinatorB.CollisionResolved += (_, e) =>
        {
            lock (collisionDecisionsB) collisionDecisionsB.Add(e);
        };

        // Register peers in each other's registry as Discovered
        coordinatorA.Registry.RegisterOrUpdateStatic(peerIdB, new IPEndPoint(IPAddress.Loopback, 5002), out _);
        coordinatorB.Registry.RegisterOrUpdateStatic(peerIdA, new IPEndPoint(IPAddress.Loopback, 5001), out _);

        // Barrier to trigger simultaneous dials
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        _output.WriteLine("Triggering simultaneous cross-dial between node-alpha and node-beta...");
        var dialTaskA = Task.Run(async () => await coordinatorA.ConnectAsync(peerIdB, ct: cts.Token));
        var dialTaskB = Task.Run(async () => await coordinatorB.ConnectAsync(peerIdA, ct: cts.Token));

        var results = await Task.WhenAll(dialTaskA, dialTaskB);
        var channelA = results[0];
        var channelB = results[1];

        // Give any background disposal tasks a moment to complete
        await Task.Delay(50);

        _output.WriteLine($"Dial completed. A channel: {channelA?.LocalPeerId}->{channelA?.RemotePeerId} (Inbound={channelA?.IsInbound})");
        _output.WriteLine($"Dial completed. B channel: {channelB?.LocalPeerId}->{channelB?.RemotePeerId} (Inbound={channelB?.IsInbound})");

        // 1. Assert exactly one active channel exists on both nodes
        coordinatorA.ActiveConnections.Count.Should().Be(1, "node-alpha must have exactly 1 active connection (P1)");
        coordinatorB.ActiveConnections.Count.Should().Be(1, "node-beta must have exactly 1 active connection (P1)");

        coordinatorA.TryGetConnection(peerIdB, out var activeAtA).Should().BeTrue();
        coordinatorB.TryGetConnection(peerIdA, out var activeAtB).Should().BeTrue();

        // 2. Assert Node B's outbound connection was preserved
        activeAtB!.IsInbound.Should().BeFalse("Node Beta is higher and must preserve its outbound connection");
        activeAtA!.IsInbound.Should().BeTrue("Node Alpha yielded and accepted Node Beta's inbound connection");

        // 3. Assert Node A's outbound dial was aborted and its channel disposed (zero socket leaks)
        alphaOutboundCreated.Should().NotBeNull();
        alphaOutboundCreated!.IsDisposed.Should().BeTrue("Node Alpha's aborted outbound dial must be disposed (I1)");

        // 4. Assert Node Beta's outbound channel is active and alive
        betaOutboundCreated.Should().NotBeNull();
        betaOutboundCreated!.IsConnected.Should().BeTrue();
        betaOutboundCreated.IsDisposed.Should().BeFalse();

        // 5. Assert peer lifecycle state in PeerRegistry is Connected on both sides
        coordinatorA.Registry.TryGetPeer(peerIdB, out var peerRecordB).Should().BeTrue();
        peerRecordB!.State.Should().Be(PeerState.Connected);

        coordinatorB.Registry.TryGetPeer(peerIdA, out var peerRecordA).Should().BeTrue();
        peerRecordA!.State.Should().Be(PeerState.Connected);

        // 6. Test bidirectional payload exchange across the surviving canonical channel
        byte[] pingMsg = "ping-from-beta"u8.ToArray();
        await activeAtB.SendAsync(pingMsg, cts.Token);
        var receivedByA = await activeAtA.ReceiveAsync(cts.Token);
        receivedByA.ToArray().Should().Equal(pingMsg);

        byte[] pongMsg = "pong-from-alpha"u8.ToArray();
        await activeAtA.SendAsync(pongMsg, cts.Token);
        var receivedByB = await activeAtB.ReceiveAsync(cts.Token);
        receivedByB.ToArray().Should().Equal(pongMsg);

        await coordinatorA.DisposeAsync();
        await coordinatorB.DisposeAsync();
    }

    [Fact]
    public async Task SpecSection10_Check2_ClusterIsolation_MismatchRejected()
    {
        // Spec §10 Check 2:
        // A node with cluster UUID C1 rejects connection attempts from a node with cluster UUID C2
        // with an explicit cluster mismatch protocol error (Postcondition P2).

        var clusterC1 = Guid.NewGuid();
        var clusterC2 = Guid.NewGuid();

        using var coordinatorC1 = new PeerConnectionCoordinator(clusterC1, "node-c1");
        using var coordinatorC2 = new PeerConnectionCoordinator(
            clusterC2,
            "node-c2",
            dialer: (remoteId, _, _) =>
            {
                var (outbound, inbound) = InMemoryTransportChannel.CreateConnectedPair("node-c2", remoteId, clusterC2);
                _ = Task.Run(async () => await coordinatorC1.AcceptConnectionAsync(inbound));
                return ValueTask.FromResult<IPeerTransportChannel>(outbound);
            });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Dial from C2 into C1
        var act = async () => await coordinatorC2.ConnectAsync("node-c1", ct: cts.Token);

        // Must fail with handshake rejection indicating cluster mismatch
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*ClusterMismatch*");

        // Assert neither coordinator registered the connection
        coordinatorC1.ActiveConnections.Should().BeEmpty("C1 must reject mismatched cluster (P2)");
        coordinatorC2.ActiveConnections.Should().BeEmpty("C2 must not have active channel to mismatched cluster (P2)");

        coordinatorC1.Registry.ActiveCount.Should().Be(0);
        coordinatorC2.Registry.ActiveCount.Should().Be(0);
    }

    [Fact]
    public async Task SequentialDial_EstablishesCanonicalChannel()
    {
        var clusterId = Guid.NewGuid();
        string peerIdA = "node-alpha";
        string peerIdB = "node-beta";

        PeerConnectionCoordinator? coordinatorB = null;

        using var coordinatorA = new PeerConnectionCoordinator(
            clusterId,
            peerIdA,
            dialer: (remoteId, _, _) =>
            {
                var (localOut, remoteIn) = InMemoryTransportChannel.CreateConnectedPair(peerIdA, remoteId, clusterId);
                _ = Task.Run(async () => await coordinatorB!.AcceptConnectionAsync(remoteIn));
                return ValueTask.FromResult<IPeerTransportChannel>(localOut);
            });

        coordinatorB = new PeerConnectionCoordinator(clusterId, peerIdB);

        var channel = await coordinatorA.ConnectAsync(peerIdB);
        channel.Should().NotBeNull();
        channel!.IsConnected.Should().BeTrue();

        coordinatorA.ActiveConnections.Count.Should().Be(1);
        coordinatorB.ActiveConnections.Count.Should().Be(1);

        coordinatorA.Registry.TryGetPeer(peerIdB, out var peerB).Should().BeTrue();
        peerB!.State.Should().Be(PeerState.Connected);

        await coordinatorB.DisposeAsync();
    }

    [Fact]
    public async Task DuplicateIncomingConnection_IsRejected_PreservingOriginal()
    {
        var clusterId = Guid.NewGuid();
        using var coordinator = new PeerConnectionCoordinator(clusterId, "node-local");

        var (client1, server1) = InMemoryTransportChannel.CreateConnectedPair("node-remote", "node-local", clusterId);
        var (client2, server2) = InMemoryTransportChannel.CreateConnectedPair("node-remote", "node-local", clusterId);

        // First connection accepted
        var accept1Task = coordinator.AcceptConnectionAsync(server1);
        var req1 = new HandshakeRequest(clusterId, "node-remote");
        await client1.SendAsync(req1.ToByteArray());
        bool accepted1 = await accept1Task;
        accepted1.Should().BeTrue();

        var resp1Bytes = await client1.ReceiveAsync();
        HandshakeResponse.TryParse(resp1Bytes.Span, out var resp1, out _).Should().BeTrue();
        resp1!.Status.Should().Be(HandshakeStatus.Success);

        // Second connection attempt from same PeerId while first is active
        var accept2Task = coordinator.AcceptConnectionAsync(server2);
        var req2 = new HandshakeRequest(clusterId, "node-remote");
        await client2.SendAsync(req2.ToByteArray());
        bool accepted2 = await accept2Task;
        accepted2.Should().BeFalse();

        var resp2Bytes = await client2.ReceiveAsync();
        HandshakeResponse.TryParse(resp2Bytes.Span, out var resp2, out _).Should().BeTrue();
        resp2!.Status.Should().Be(HandshakeStatus.DuplicateConnection);

        // Server2 should be disposed, Server1 remains connected
        server2.IsDisposed.Should().BeTrue("Duplicate connection channel must be disposed (I1)");
        server1.IsConnected.Should().BeTrue();
        coordinator.ActiveConnections.Count.Should().Be(1);
    }

    [Fact]
    public async Task IncompatibleProtocolVersion_IsRejected()
    {
        var clusterId = Guid.NewGuid();
        using var coordinator = new PeerConnectionCoordinator(clusterId, "node-local");

        var (client, server) = InMemoryTransportChannel.CreateConnectedPair("node-remote", "node-local", clusterId);

        var acceptTask = coordinator.AcceptConnectionAsync(server);
        var req = new HandshakeRequest(clusterId, "node-remote", ProtocolVersion: 99);
        await client.SendAsync(req.ToByteArray());

        bool accepted = await acceptTask;
        accepted.Should().BeFalse();

        var respBytes = await client.ReceiveAsync();
        HandshakeResponse.TryParse(respBytes.Span, out var resp, out _).Should().BeTrue();
        resp!.Status.Should().Be(HandshakeStatus.VersionIncompatible);
        server.IsDisposed.Should().BeTrue();
    }

    [Fact]
    public async Task CloseConnectionAsync_DisconnectsAndEmitsEvent()
    {
        var clusterId = Guid.NewGuid();
        using var coordinator = new PeerConnectionCoordinator(clusterId, "node-local");

        var (client, server) = InMemoryTransportChannel.CreateConnectedPair("node-remote", "node-local", clusterId);
        var acceptTask = coordinator.AcceptConnectionAsync(server);
        var req = new HandshakeRequest(clusterId, "node-remote");
        await client.SendAsync(req.ToByteArray());
        await acceptTask;

        coordinator.ActiveConnections.Count.Should().Be(1);

        string? closedPeerId = null;
        coordinator.ConnectionClosed += (_, peer) => closedPeerId = peer;

        await coordinator.CloseConnectionAsync("node-remote", "User initiated");

        closedPeerId.Should().Be("node-remote");
        coordinator.ActiveConnections.Should().BeEmpty();
        server.IsConnected.Should().BeFalse();
    }

    [Fact]
    public async Task ConnectAsync_MultiplexedCallers_Caller2CancellationDoesNotAbortCaller1Dial()
    {
        var clusterId = Guid.NewGuid();
        string peerIdA = "node-alpha";
        string peerIdB = "node-beta";

        PeerConnectionCoordinator? coordinatorB = null;
        var dialStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var coordinatorA = new PeerConnectionCoordinator(
            clusterId,
            peerIdA,
            dialer: async (remoteId, endpoint, ct) =>
            {
                dialStarted.TrySetResult();
                // Delay to allow Caller 2 to join and cancel before dial finishes
                await Task.Delay(150, ct);
                var (localOut, remoteIn) = InMemoryTransportChannel.CreateConnectedPair(peerIdA, remoteId, clusterId);
                _ = Task.Run(async () => await coordinatorB!.AcceptConnectionAsync(remoteIn));
                return localOut;
            });

        coordinatorB = new PeerConnectionCoordinator(clusterId, peerIdB);

        using var ctsCaller1 = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var ctsCaller2 = new CancellationTokenSource(TimeSpan.FromMilliseconds(30));

        // Caller 1 starts dial
        var caller1Task = coordinatorA.ConnectAsync(peerIdB, ct: ctsCaller1.Token);

        // Wait until dialer has started
        await dialStarted.Task;

        // Caller 2 joins dial with short cancellation token
        var caller2Task = coordinatorA.ConnectAsync(peerIdB, ct: ctsCaller2.Token);

        // Caller 2 should throw OperationCanceledException
        var actCaller2 = async () => await caller2Task;
        await actCaller2.Should().ThrowAsync<OperationCanceledException>();

        // Caller 1 must succeed despite Caller 2's cancellation
        var channel1 = await caller1Task;
        channel1.Should().NotBeNull();
        channel1!.IsConnected.Should().BeTrue();

        await coordinatorB.DisposeAsync();
    }
}

