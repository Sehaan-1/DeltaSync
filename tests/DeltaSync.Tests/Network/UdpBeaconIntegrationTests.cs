namespace DeltaSync.Tests.Network;

using System.Net;
using System.Net.Sockets;
using DeltaSync.Network;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

public class UdpBeaconIntegrationTests
{
    private readonly ITestOutputHelper _output;

    public UdpBeaconIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    private static int GetAvailableUdpPort()
    {
        using var udp = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        return ((IPEndPoint)udp.Client.LocalEndPoint!).Port;
    }

    [Fact]
    public async Task SpecSection10_Check1_UdpBeacon_LoopbackSuppression_FiltersSelfBeacons()
    {
        // 1. Configure both listener and announcer with identical PeerId
        int port = GetAvailableUdpPort();
        var clusterId = Guid.NewGuid();
        string peerId = "local-node-charlie";

        var listenerOptions = new BeaconOptions
        {
            MulticastAddress = IPAddress.Loopback,
            MulticastPort = port,
            ClusterId = clusterId,
            PeerId = peerId,
            ListenPort = 5001
        };

        var announcerOptions = new BeaconOptions
        {
            MulticastAddress = IPAddress.Loopback,
            MulticastPort = port,
            ClusterId = clusterId,
            PeerId = peerId,
            ListenPort = 5001
        };

        await using var listener = new UdpBeaconListener(listenerOptions);
        var receivedBeacons = new List<DiscoveredBeacon>();
        listener.BeaconReceived += (s, beacon) =>
        {
            lock (receivedBeacons)
            {
                receivedBeacons.Add(beacon);
            }
        };

        listener.Start();

        await using var announcer = new UdpBeaconAnnouncer(announcerOptions);

        // 2. Broadcast 3 consecutive self-beacons
        for (int i = 0; i < 3; i++)
        {
            await announcer.BroadcastOnceAsync();
            await Task.Delay(50);
        }

        // Wait brief window for socket delivery
        await Task.Delay(200);

        // 3. Assert Invariant I3: Self-beacons must be suppressed and NEVER emitted as discovered peers
        lock (receivedBeacons)
        {
            receivedBeacons.Should().BeEmpty("Self-emitted beacons must be filtered by loopback suppression (Invariant I3)");
        }

        listener.PacketsReceived.Should().BeGreaterOrEqualTo(3, "Packets must be received at socket layer");
        listener.SelfBeaconsSuppressed.Should().BeGreaterOrEqualTo(3, "Self beacons must be recognized and suppressed");
        listener.DroppedPackets.Should().Be(0, "Valid self beacons are accounted in SelfBeaconsSuppressed, not DroppedPackets");

        _output.WriteLine($"[Check 1 Point 3 Passed] Received: {listener.PacketsReceived}, Suppressed: {listener.SelfBeaconsSuppressed}, Discovered: {receivedBeacons.Count}");
    }

    [Fact]
    public async Task UdpBeacon_RemotePeerDiscovered_EmitsEventWithCorrectMetadata()
    {
        int port = GetAvailableUdpPort();
        var clusterId = Guid.NewGuid();

        var listenerOptions = new BeaconOptions
        {
            MulticastAddress = IPAddress.Loopback,
            MulticastPort = port,
            ClusterId = clusterId,
            PeerId = "node-receiver",
            ListenPort = 5001
        };

        var announcerOptions = new BeaconOptions
        {
            MulticastAddress = IPAddress.Loopback,
            MulticastPort = port,
            ClusterId = clusterId,
            PeerId = "node-sender",
            ListenPort = 5002
        };

        await using var listener = new UdpBeaconListener(listenerOptions);
        var tcs = new TaskCompletionSource<DiscoveredBeacon>(TaskCreationOptions.RunContinuationsAsynchronously);

        listener.BeaconReceived += (s, beacon) =>
        {
            tcs.TrySetResult(beacon);
        };

        listener.Start();

        await using var announcer = new UdpBeaconAnnouncer(announcerOptions);
        await announcer.BroadcastOnceAsync();

        var completedTask = await Task.WhenAny(tcs.Task, Task.Delay(2000));
        completedTask.Should().Be(tcs.Task, "Listener should discover remote peer beacon within timeout");

        var discovered = await tcs.Task;
        discovered.Should().NotBeNull();
        discovered.Frame.ClusterId.Should().Be(clusterId);
        discovered.Frame.ListenPort.Should().Be(5002);
        discovered.Frame.PeerIdHash.Should().Equal(BeaconFrame.ComputePeerIdHash("node-sender"));
        discovered.ServiceEndPoint.Port.Should().Be(5002);
        discovered.ServiceEndPoint.Address.Should().Be(IPAddress.Loopback);

        _output.WriteLine($"[Remote Peer Discovered] Sender port: {discovered.ServiceEndPoint.Port}, Hash: {discovered.Frame.PeerIdHashHex[..8]}...");
    }

    [Fact]
    public async Task UdpBeacon_ClusterIsolation_SilentlyDropsDifferentCluster()
    {
        int port = GetAvailableUdpPort();
        var clusterA = Guid.NewGuid();
        var clusterB = Guid.NewGuid();

        var listenerOptions = new BeaconOptions
        {
            MulticastAddress = IPAddress.Loopback,
            MulticastPort = port,
            ClusterId = clusterA,
            PeerId = "node-in-cluster-a",
            ListenPort = 5001
        };

        var announcerOptions = new BeaconOptions
        {
            MulticastAddress = IPAddress.Loopback,
            MulticastPort = port,
            ClusterId = clusterB,
            PeerId = "node-in-cluster-b",
            ListenPort = 5002
        };

        await using var listener = new UdpBeaconListener(listenerOptions);
        var received = new List<DiscoveredBeacon>();
        listener.BeaconReceived += (s, beacon) => received.Add(beacon);
        listener.Start();

        await using var announcer = new UdpBeaconAnnouncer(announcerOptions);
        await announcer.BroadcastOnceAsync();

        await Task.Delay(200);

        received.Should().BeEmpty("Packets from different ClusterId must be dropped silently (Postcondition P2)");
        listener.DroppedPackets.Should().BeGreaterOrEqualTo(1);

        _output.WriteLine($"[Cluster Isolation Passed] Dropped foreign cluster packet: {listener.DroppedPackets}");
    }

    [Fact]
    public async Task UdpBeacon_MalformedDatagrams_DroppedSafely()
    {
        int port = GetAvailableUdpPort();
        var clusterId = Guid.NewGuid();

        var listenerOptions = new BeaconOptions
        {
            MulticastAddress = IPAddress.Loopback,
            MulticastPort = port,
            ClusterId = clusterId,
            PeerId = "node-listener",
            ListenPort = 5001
        };

        await using var listener = new UdpBeaconListener(listenerOptions);
        listener.Start();

        // Send raw malformed datagram (e.g. 10 garbage bytes)
        using var client = new UdpClient();
        byte[] garbage = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        await client.SendAsync(garbage, garbage.Length, new IPEndPoint(IPAddress.Loopback, port));

        await Task.Delay(150);

        listener.PacketsReceived.Should().BeGreaterOrEqualTo(1);
        listener.DroppedPackets.Should().BeGreaterOrEqualTo(1, "Malformed datagrams must be accounted in DroppedPackets");

        _output.WriteLine($"[Malformed Datagrams Passed] Successfully dropped garbage bytes: {listener.DroppedPackets}");
    }
}
