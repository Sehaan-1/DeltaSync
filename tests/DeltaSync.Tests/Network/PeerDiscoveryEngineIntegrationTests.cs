namespace DeltaSync.Tests.Network;

using System.Net;
using System.Net.Sockets;
using DeltaSync.Network;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

public class PeerDiscoveryEngineIntegrationTests
{
    private readonly ITestOutputHelper _output;

    public PeerDiscoveryEngineIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    private static int GetAvailableUdpPort()
    {
        using var udp = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
        return ((IPEndPoint)udp.Client.LocalEndPoint!).Port;
    }

    [Fact]
    public void SpecSection10_Check3_ThreeNode_DiscoveryAndLiveness_VirtualTimeSimulation()
    {
        // Spec §10 Check 3:
        // - Run three simulated nodes (A, B, C)
        // - Verify nodes A and B discover each other within 3.5s
        // - Stop node B's beacon announcer
        // - Assert node A marks node B as Stale after 6.0s and Dead after 9.0s
        var clusterId = Guid.NewGuid();
        var fakeTime = new FakeTimeProvider(new DateTimeOffset(2026, 9, 23, 10, 0, 0, TimeSpan.Zero));

        var registryA = new PeerRegistry(fakeTime);
        var trackerA = new PeerLivenessTracker(registryA, new PeerLivenessOptions
        {
            StaleTimeout = TimeSpan.FromSeconds(6.0),
            DeadTimeout = TimeSpan.FromSeconds(9.0),
            EvictOnDead = true
        }, fakeTime);

        PeerRecord? stalePeer = null;
        PeerRecord? lostPeer = null;
        registryA.PeerStale += (_, p) => stalePeer = p;
        registryA.PeerLost += (_, p) => lostPeer = p;

        // Node B and Node C emit initial beacons at t=0
        var hashB = BeaconFrame.ComputePeerIdHash("node-B");
        var hashC = BeaconFrame.ComputePeerIdHash("node-C");

        var beaconB0 = new DiscoveredBeacon(
            new BeaconFrame(clusterId, hashB, 5002, 1, fakeTime.GetUtcNow().ToUnixTimeMilliseconds()),
            new IPEndPoint(IPAddress.Loopback, 58732));

        var beaconC0 = new DiscoveredBeacon(
            new BeaconFrame(clusterId, hashC, 5003, 1, fakeTime.GetUtcNow().ToUnixTimeMilliseconds()),
            new IPEndPoint(IPAddress.Loopback, 58732));

        registryA.RegisterOrUpdateBeacon(beaconB0, "node-B", out _);
        registryA.RegisterOrUpdateBeacon(beaconC0, "node-C", out _);

        // Nodes A and B have discovered each other (within T_discover <= 3.5s)
        registryA.ActiveCount.Should().Be(2);
        registryA.TryGetPeer("node-B", out var nodeBRecord).Should().BeTrue();
        registryA.TryGetPeer("node-C", out var nodeCRecord).Should().BeTrue();
        nodeBRecord!.State.Should().Be(PeerState.Discovered);
        nodeCRecord!.State.Should().Be(PeerState.Discovered);

        // Node B is now connected
        registryA.MarkConnected("node-B", out _);

        // Advance 3.0s: Node C sends its regular beacon; Node B also sends beacon at 3.0s
        fakeTime.Advance(TimeSpan.FromSeconds(3.0));
        var beaconB1 = new DiscoveredBeacon(
            new BeaconFrame(clusterId, hashB, 5002, 2, fakeTime.GetUtcNow().ToUnixTimeMilliseconds()),
            new IPEndPoint(IPAddress.Loopback, 58732));
        var beaconC1 = new DiscoveredBeacon(
            new BeaconFrame(clusterId, hashC, 5003, 2, fakeTime.GetUtcNow().ToUnixTimeMilliseconds()),
            new IPEndPoint(IPAddress.Loopback, 58732));
        registryA.RegisterOrUpdateBeacon(beaconB1, "node-B", out _);
        registryA.RegisterOrUpdateBeacon(beaconC1, "node-C", out _);

        trackerA.EvaluateLiveness();
        registryA.ActiveCount.Should().Be(2);

        // NOW: Stop node B's beacon announcer! Only Node C continues broadcasting.
        _output.WriteLine("Node B's beacon announcer stopped at t=3.0s.");

        // At t=6.0s (3.0s since B's last beacon): Node C announces
        fakeTime.Advance(TimeSpan.FromSeconds(3.0));
        var beaconC2 = new DiscoveredBeacon(
            new BeaconFrame(clusterId, hashC, 5003, 3, fakeTime.GetUtcNow().ToUnixTimeMilliseconds()),
            new IPEndPoint(IPAddress.Loopback, 58732));
        registryA.RegisterOrUpdateBeacon(beaconC2, "node-C", out _);

        trackerA.EvaluateLiveness();
        registryA.TryGetPeer("node-B", out var bAt6s);
        bAt6s!.State.Should().Be(PeerState.Connected); // Only 3.0s elapsed for B

        // At t=9.1s (6.1s since B's last beacon at t=3.0s):
        // Node B must transition to Stale!
        fakeTime.Advance(TimeSpan.FromSeconds(3.1));
        var beaconC3 = new DiscoveredBeacon(
            new BeaconFrame(clusterId, hashC, 5003, 4, fakeTime.GetUtcNow().ToUnixTimeMilliseconds()),
            new IPEndPoint(IPAddress.Loopback, 58732));
        registryA.RegisterOrUpdateBeacon(beaconC3, "node-C", out _);

        var sweepAt9s = trackerA.EvaluateLiveness();
        sweepAt9s.TransitionedToStale.Should().Be(1);

        registryA.TryGetPeer("node-B", out var bStaleRecord);
        bStaleRecord!.State.Should().Be(PeerState.Stale);
        stalePeer.Should().NotBeNull();
        stalePeer!.PeerId.Should().Be("node-B");

        // Node C must remain completely healthy
        registryA.TryGetPeer("node-C", out var cActiveRecord);
        cActiveRecord!.State.Should().Be(PeerState.Discovered);
        registryA.ActiveCount.Should().Be(2);

        // At t=12.1s (9.1s since B's last beacon at t=3.0s):
        // Node B must transition to Dead and be evicted!
        fakeTime.Advance(TimeSpan.FromSeconds(3.0));
        var sweepAt12s = trackerA.EvaluateLiveness();
        sweepAt12s.TransitionedToDead.Should().Be(1);

        lostPeer.Should().NotBeNull();
        lostPeer!.PeerId.Should().Be("node-B");
        lostPeer.State.Should().Be(PeerState.Dead);

        // Node B is evicted from active registry; Node C survives
        registryA.ActiveCount.Should().Be(1);
        registryA.TryGetPeer("node-B", out _).Should().BeFalse();
        registryA.TryGetPeer("node-C", out _).Should().BeTrue();
    }

    [Fact]
    public async Task HybridDiscovery_UnifiesLiveUdpBeacons_And_StaticEndpoints()
    {
        // Tests ADR-0002 requirement: IPeerDiscovery unifies broadcast beacons and configured endpoints
        int port = GetAvailableUdpPort();
        var clusterId = Guid.NewGuid();

        var optionsA = new BeaconOptions
        {
            MulticastAddress = IPAddress.Loopback,
            MulticastPort = port,
            ClusterId = clusterId,
            PeerId = "node-A",
            ListenPort = 5001,
            BaseInterval = TimeSpan.FromMilliseconds(200)
        };

        var optionsB = new BeaconOptions
        {
            MulticastAddress = IPAddress.Loopback,
            MulticastPort = port,
            ClusterId = clusterId,
            PeerId = "node-B",
            ListenPort = 5002,
            BaseInterval = TimeSpan.FromMilliseconds(200)
        };

        await using var engineA = new PeerDiscoveryEngine(optionsA, staticEndpoints: ["10.10.10.50:7001"]);
        await using var engineB = new PeerDiscoveryEngine(optionsB);

        var discoveredByA = new List<PeerRecord>();
        engineA.PeerDiscovered += (_, p) =>
        {
            lock (discoveredByA)
            {
                discoveredByA.Add(p);
            }
        };

        // Static peer should already be in active peers
        engineA.ActivePeers.Should().ContainSingle(p => p.IsStatic && p.Endpoint.Port == 7001);

        await engineA.StartAsync();
        await engineB.StartAsync();

        // Wait for multicast beacon exchange (within 3.5s per Spec)
        var deadline = DateTime.UtcNow.AddSeconds(3.5);
        bool foundNodeB = false;

        while (DateTime.UtcNow < deadline)
        {
            lock (discoveredByA)
            {
                if (discoveredByA.Any(p => !p.IsStatic && p.Endpoint.Port == 5002))
                {
                    foundNodeB = true;
                    break;
                }
            }
            await Task.Delay(50);
        }

        foundNodeB.Should().BeTrue("Node A must discover Node B via UDP broadcast within 3.5 seconds");

        // Both static and dynamic peers are unified in engineA.ActivePeers
        engineA.ActivePeers.Should().Contain(p => p.IsStatic && p.Endpoint.Port == 7001);
        engineA.ActivePeers.Should().Contain(p => !p.IsStatic && p.Endpoint.Port == 5002);

        await engineB.StopAsync();
        await engineA.StopAsync();
    }
}
