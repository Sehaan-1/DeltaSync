namespace DeltaSync.Tests.Network;

using System.Net;
using DeltaSync.Network;
using FluentAssertions;
using Xunit;

public class PeerLivenessTrackerTests
{
    private readonly Guid _clusterId = Guid.NewGuid();

    [Fact]
    public void SpecSection10_Check3_PeerRegistry_LivenessTimeout_TransitionsToStaleAndDead()
    {
        var fakeTime = new FakeTimeProvider(new DateTimeOffset(2026, 9, 23, 12, 0, 0, TimeSpan.Zero));
        var registry = new PeerRegistry(fakeTime);
        var tracker = new PeerLivenessTracker(registry, new PeerLivenessOptions
        {
            StaleTimeout = TimeSpan.FromSeconds(6.0),
            DeadTimeout = TimeSpan.FromSeconds(9.0),
            EvictOnDead = true
        }, fakeTime);

        PeerRecord? staleReceived = null;
        PeerRecord? lostReceived = null;
        registry.PeerStale += (_, p) => staleReceived = p;
        registry.PeerLost += (_, p) => lostReceived = p;

        // 1. Initial discovery at t=0
        var hash = BeaconFrame.ComputePeerIdHash("peer-node-1");
        var frame = new BeaconFrame(_clusterId, hash, 5001, 1, fakeTime.GetUtcNow().ToUnixTimeMilliseconds());
        var beacon = new DiscoveredBeacon(frame, new IPEndPoint(IPAddress.Loopback, 58732));

        registry.RegisterOrUpdateBeacon(beacon, "peer-node-1", out _);
        registry.MarkConnected("peer-node-1", out var initialRecord);
        initialRecord!.State.Should().Be(PeerState.Connected);
        registry.ActiveCount.Should().Be(1);

        // 2. Advance time to 5.9s (under 6.0s stale threshold)
        fakeTime.Advance(TimeSpan.FromSeconds(5.9));
        var sweep1 = tracker.EvaluateLiveness();

        sweep1.TransitionedToStale.Should().Be(0);
        sweep1.TransitionedToDead.Should().Be(0);
        registry.TryGetPeer("peer-node-1", out var recordAt5s);
        recordAt5s!.State.Should().Be(PeerState.Connected);
        staleReceived.Should().BeNull();

        // 3. Advance time to 6.1s (exceeds 6.0s stale threshold)
        fakeTime.Advance(TimeSpan.FromSeconds(0.2));
        var sweep2 = tracker.EvaluateLiveness();

        sweep2.TransitionedToStale.Should().Be(1);
        sweep2.TransitionedToDead.Should().Be(0);
        registry.TryGetPeer("peer-node-1", out var recordAt6s);
        recordAt6s!.State.Should().Be(PeerState.Stale);
        staleReceived.Should().NotBeNull();
        staleReceived!.PeerId.Should().Be("peer-node-1");
        registry.ActiveCount.Should().Be(1); // Still active while stale

        // 4. Advance time to 8.9s (under 9.0s dead threshold)
        fakeTime.Advance(TimeSpan.FromSeconds(2.8));
        var sweep3 = tracker.EvaluateLiveness();

        sweep3.TransitionedToStale.Should().Be(0);
        sweep3.TransitionedToDead.Should().Be(0);
        registry.TryGetPeer("peer-node-1", out var recordAt8s);
        recordAt8s!.State.Should().Be(PeerState.Stale);
        lostReceived.Should().BeNull();

        // 5. Advance time to 9.1s (exceeds 9.0s dead threshold)
        fakeTime.Advance(TimeSpan.FromSeconds(0.2));
        var sweep4 = tracker.EvaluateLiveness();

        sweep4.TransitionedToDead.Should().Be(1);
        lostReceived.Should().NotBeNull();
        lostReceived!.PeerId.Should().Be("peer-node-1");
        lostReceived.State.Should().Be(PeerState.Dead);

        // Assert peer is evicted from active registry (Postcondition P3)
        registry.ActiveCount.Should().Be(0);
        registry.TryGetPeer("peer-node-1", out _).Should().BeFalse();
    }

    [Fact]
    public void LivenessTimeout_PeerReceivesBeaconWhileStale_RecoversToDiscovered()
    {
        var fakeTime = new FakeTimeProvider();
        var registry = new PeerRegistry(fakeTime);
        var tracker = new PeerLivenessTracker(registry, new PeerLivenessOptions
        {
            StaleTimeout = TimeSpan.FromSeconds(6.0),
            DeadTimeout = TimeSpan.FromSeconds(9.0)
        }, fakeTime);

        var hash = BeaconFrame.ComputePeerIdHash("peer-node-recover");
        var frame = new BeaconFrame(_clusterId, hash, 5001, 1, 0);
        var beacon = new DiscoveredBeacon(frame, new IPEndPoint(IPAddress.Loopback, 58732));

        registry.RegisterOrUpdateBeacon(beacon, "peer-node-recover", out _);

        // Advance to 6.5s -> Peer becomes Stale
        fakeTime.Advance(TimeSpan.FromSeconds(6.5));
        tracker.EvaluateLiveness();
        registry.TryGetPeer("peer-node-recover", out var stalePeer);
        stalePeer!.State.Should().Be(PeerState.Stale);

        // A new beacon arrives at t=7.0s
        fakeTime.Advance(TimeSpan.FromSeconds(0.5));
        var frame2 = new BeaconFrame(_clusterId, hash, 5001, 2, 0);
        var beacon2 = new DiscoveredBeacon(frame2, new IPEndPoint(IPAddress.Loopback, 58732));
        registry.RegisterOrUpdateBeacon(beacon2, "peer-node-recover", out var recoveredPeer);

        recoveredPeer.State.Should().Be(PeerState.Discovered);

        // Advance another 5 seconds to t=12.0s (5s elapsed since t=7.0s beacon)
        fakeTime.Advance(TimeSpan.FromSeconds(5.0));
        var sweep = tracker.EvaluateLiveness();

        sweep.TransitionedToStale.Should().Be(0);
        sweep.TransitionedToDead.Should().Be(0);
        registry.TryGetPeer("peer-node-recover", out var currentPeer);
        currentPeer!.State.Should().Be(PeerState.Discovered);
        registry.ActiveCount.Should().Be(1);
    }

    [Fact]
    public void LivenessTimeout_MultiplePeers_StaggeredExpirations()
    {
        var fakeTime = new FakeTimeProvider();
        var registry = new PeerRegistry(fakeTime);
        var tracker = new PeerLivenessTracker(registry, new PeerLivenessOptions
        {
            StaleTimeout = TimeSpan.FromSeconds(6.0),
            DeadTimeout = TimeSpan.FromSeconds(9.0)
        }, fakeTime);

        // Register Node A at t=0
        var hashA = BeaconFrame.ComputePeerIdHash("node-A");
        registry.RegisterOrUpdateBeacon(
            new DiscoveredBeacon(new BeaconFrame(_clusterId, hashA, 5001, 1, 0), new IPEndPoint(IPAddress.Loopback, 58732)),
            "node-A", out _);

        // Advance 3 seconds, register Node B at t=3
        fakeTime.Advance(TimeSpan.FromSeconds(3.0));
        var hashB = BeaconFrame.ComputePeerIdHash("node-B");
        registry.RegisterOrUpdateBeacon(
            new DiscoveredBeacon(new BeaconFrame(_clusterId, hashB, 5002, 1, 0), new IPEndPoint(IPAddress.Loopback, 58732)),
            "node-B", out _);

        // At t=6.5s (A has elapsed 6.5s, B has elapsed 3.5s)
        fakeTime.Advance(TimeSpan.FromSeconds(3.5));
        var sweep1 = tracker.EvaluateLiveness();
        sweep1.TransitionedToStale.Should().Be(1); // Node A becomes stale
        sweep1.TransitionedToDead.Should().Be(0);

        registry.TryGetPeer("node-A", out var peerA);
        registry.TryGetPeer("node-B", out var peerB);
        peerA!.State.Should().Be(PeerState.Stale);
        peerB!.State.Should().Be(PeerState.Discovered);

        // At t=9.5s (A has elapsed 9.5s -> Dead; B has elapsed 6.5s -> Stale)
        fakeTime.Advance(TimeSpan.FromSeconds(3.0));
        var sweep2 = tracker.EvaluateLiveness();
        sweep2.TransitionedToStale.Should().Be(1); // Node B becomes stale
        sweep2.TransitionedToDead.Should().Be(1);  // Node A becomes dead

        registry.TryGetPeer("node-A", out _).Should().BeFalse(); // A evicted
        registry.TryGetPeer("node-B", out peerB);
        peerB!.State.Should().Be(PeerState.Stale);
        registry.ActiveCount.Should().Be(1);

        // At t=12.5s (B has elapsed 9.5s -> Dead)
        fakeTime.Advance(TimeSpan.FromSeconds(3.0));
        var sweep3 = tracker.EvaluateLiveness();
        sweep3.TransitionedToDead.Should().Be(1);  // Node B becomes dead

        registry.ActiveCount.Should().Be(0);
    }
}
