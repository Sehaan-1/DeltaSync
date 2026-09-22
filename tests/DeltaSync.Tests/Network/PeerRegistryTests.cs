namespace DeltaSync.Tests.Network;

using System.Net;
using DeltaSync.Network;
using FluentAssertions;
using Xunit;

public class PeerRegistryTests
{
    private readonly Guid _clusterId = Guid.NewGuid();

    [Fact]
    public void RegisterOrUpdateBeacon_NewPeer_InsertsAndFiresDiscoveredEvent()
    {
        var registry = new PeerRegistry();
        PeerRecord? discoveredEventArg = null;
        registry.PeerDiscovered += (_, p) => discoveredEventArg = p;

        var frame = new BeaconFrame(
            _clusterId,
            BeaconFrame.ComputePeerIdHash("peer-alpha"),
            5001,
            1,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        var beacon = new DiscoveredBeacon(frame, new IPEndPoint(IPAddress.Parse("192.168.1.100"), 58732));

        bool isNew = registry.RegisterOrUpdateBeacon(beacon, "peer-alpha", out var peer);

        isNew.Should().BeTrue();
        peer.PeerId.Should().Be("peer-alpha");
        peer.Endpoint.Should().Be(new IPEndPoint(IPAddress.Parse("192.168.1.100"), 5001));
        peer.State.Should().Be(PeerState.Discovered);
        peer.LastSequence.Should().Be(1UL);
        peer.IsStatic.Should().BeFalse();

        discoveredEventArg.Should().NotBeNull();
        discoveredEventArg!.PeerId.Should().Be("peer-alpha");
        registry.ActiveCount.Should().Be(1);
    }

    [Fact]
    public void RegisterOrUpdateBeacon_ExistingPeer_RefreshesLastSeenAndEndpoint()
    {
        var registry = new PeerRegistry();
        var peerIdHash = BeaconFrame.ComputePeerIdHash("peer-beta");

        var frame1 = new BeaconFrame(_clusterId, peerIdHash, 5001, 1, 1000);
        var beacon1 = new DiscoveredBeacon(frame1, new IPEndPoint(IPAddress.Parse("192.168.1.101"), 58732));
        registry.RegisterOrUpdateBeacon(beacon1, "peer-beta", out _);

        int discoveredEvents = 0;
        registry.PeerDiscovered += (_, _) => discoveredEvents++;

        // Second beacon with higher sequence and new port
        var frame2 = new BeaconFrame(_clusterId, peerIdHash, 5002, 2, 2000);
        var beacon2 = new DiscoveredBeacon(frame2, new IPEndPoint(IPAddress.Parse("192.168.1.101"), 58732));
        bool isNew = registry.RegisterOrUpdateBeacon(beacon2, "peer-beta", out var updated);

        isNew.Should().BeFalse();
        discoveredEvents.Should().Be(0);
        updated.Endpoint.Port.Should().Be(5002);
        updated.LastSequence.Should().Be(2UL);
        registry.ActiveCount.Should().Be(1);
    }

    [Fact]
    public void RegisterOrUpdateStatic_InsertsStaticPeerAndFiresDiscoveredEvent()
    {
        var registry = new PeerRegistry();
        PeerRecord? discoveredEventArg = null;
        registry.PeerDiscovered += (_, p) => discoveredEventArg = p;

        var endpoint = new IPEndPoint(IPAddress.Parse("10.0.0.5"), 6000);
        bool isNew = registry.RegisterOrUpdateStatic("static-node", endpoint, out var peer);

        isNew.Should().BeTrue();
        peer.PeerId.Should().Be("static-node");
        peer.Endpoint.Should().Be(endpoint);
        peer.IsStatic.Should().BeTrue();
        peer.State.Should().Be(PeerState.Discovered);

        discoveredEventArg.Should().NotBeNull();
        discoveredEventArg!.PeerId.Should().Be("static-node");
    }

    [Fact]
    public void LookupByPeerId_And_LookupByHash_ReturnConsistentSnapshot()
    {
        var registry = new PeerRegistry();
        var hash = BeaconFrame.ComputePeerIdHash("peer-gamma");
        var frame = new BeaconFrame(_clusterId, hash, 5003, 10, 5000);
        var beacon = new DiscoveredBeacon(frame, new IPEndPoint(IPAddress.Parse("192.168.1.102"), 58732));

        registry.RegisterOrUpdateBeacon(beacon, "peer-gamma", out _);

        bool foundById = registry.TryGetPeer("peer-gamma", out var peerById);
        bool foundByHash = registry.TryGetPeerByHash(hash, out var peerByHash);

        foundById.Should().BeTrue();
        foundByHash.Should().BeTrue();
        peerById!.PeerId.Should().Be(peerByHash!.PeerId);
        peerById.Endpoint.Should().Be(peerByHash.Endpoint);
    }

    [Fact]
    public void MarkConnected_TransitionsToConnected_ResetsConsecutiveFailures()
    {
        var registry = new PeerRegistry();
        registry.RegisterOrUpdateStatic("node-1", new IPEndPoint(IPAddress.Loopback, 5001), out _);
        registry.RecordFailure("node-1");
        registry.RecordFailure("node-1");

        PeerRecord? connectedArg = null;
        registry.PeerConnected += (_, p) => connectedArg = p;

        bool ok = registry.MarkConnected("node-1", out var connected);

        ok.Should().BeTrue();
        connected!.State.Should().Be(PeerState.Connected);
        connected.ConsecutiveFailures.Should().Be(0);
        connected.LastConnected.Should().NotBeNull();
        connectedArg.Should().NotBeNull();
        connectedArg!.State.Should().Be(PeerState.Connected);
    }

    [Fact]
    public void MarkStale_TransitionsToStale_FiresStaleEvent()
    {
        var registry = new PeerRegistry();
        registry.RegisterOrUpdateStatic("node-2", new IPEndPoint(IPAddress.Loopback, 5002), out _);

        PeerRecord? staleArg = null;
        registry.PeerStale += (_, p) => staleArg = p;

        bool ok = registry.MarkStale("node-2", out var stale);

        ok.Should().BeTrue();
        stale!.State.Should().Be(PeerState.Stale);
        staleArg.Should().NotBeNull();
        staleArg!.State.Should().Be(PeerState.Stale);
    }

    [Fact]
    public void MarkDead_TransitionsToDead_EvictsAndFiresLostEvent()
    {
        var registry = new PeerRegistry();
        registry.RegisterOrUpdateStatic("node-3", new IPEndPoint(IPAddress.Loopback, 5003), out _);

        PeerRecord? lostArg = null;
        registry.PeerLost += (_, p) => lostArg = p;

        bool ok = registry.MarkDead("node-3", evict: true, out var dead);

        ok.Should().BeTrue();
        dead!.State.Should().Be(PeerState.Dead);
        lostArg.Should().NotBeNull();
        lostArg!.State.Should().Be(PeerState.Dead);

        registry.ActiveCount.Should().Be(0);
        registry.TryGetPeer("node-3", out _).Should().BeFalse();
    }

    [Fact]
    public void StalePeer_ReceivesBeacon_TransitionsBackToDiscovered()
    {
        var registry = new PeerRegistry();
        var hash = BeaconFrame.ComputePeerIdHash("node-4");
        var frame = new BeaconFrame(_clusterId, hash, 5004, 1, 1000);
        var beacon = new DiscoveredBeacon(frame, new IPEndPoint(IPAddress.Loopback, 58732));

        registry.RegisterOrUpdateBeacon(beacon, "node-4", out _);
        registry.MarkStale("node-4", out _);

        // Next beacon arrives
        var nextFrame = new BeaconFrame(_clusterId, hash, 5004, 2, 2000);
        var nextBeacon = new DiscoveredBeacon(nextFrame, new IPEndPoint(IPAddress.Loopback, 58732));

        registry.RegisterOrUpdateBeacon(nextBeacon, "node-4", out var recovered);

        recovered.State.Should().Be(PeerState.Discovered);
    }
}
