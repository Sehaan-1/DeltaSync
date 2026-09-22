using DeltaSync.Core.Causality;
using FluentAssertions;
using Xunit;

namespace DeltaSync.Tests.Causality;

public class VectorClockTests
{
    #region Known-Answer Vector Suite (Spec §6)

    [Fact]
    public void KnownAnswer_V1_Equal()
    {
        // V1: {A: 1} vs {A: 1} => Equal
        var vA = VectorClock.Create(("A", 1));
        var vB = VectorClock.Create(("A", 1));

        vA.Compare(vB).Should().Be(CausalRelation.Equal);
        (vA == vB).Should().BeTrue();
        (vA <= vB).Should().BeTrue();
        (vA >= vB).Should().BeTrue();
        (vA < vB).Should().BeFalse();
        (vA > vB).Should().BeFalse();
    }

    [Fact]
    public void KnownAnswer_V2_Before()
    {
        // V2: {A: 1} vs {A: 2} => Before (VA < VB)
        var vA = VectorClock.Create(("A", 1));
        var vB = VectorClock.Create(("A", 2));

        vA.Compare(vB).Should().Be(CausalRelation.Before);
        (vA < vB).Should().BeTrue();
        (vA <= vB).Should().BeTrue();
        (vA > vB).Should().BeFalse();
        (vA >= vB).Should().BeFalse();
        (vA == vB).Should().BeFalse();
        vA.IsDominatedBy(vB).Should().BeTrue();
        vA.IsDominating(vB).Should().BeFalse();
    }

    [Fact]
    public void KnownAnswer_V3_After()
    {
        // V3: {A: 2} vs {A: 1} => After (VB < VA)
        var vA = VectorClock.Create(("A", 2));
        var vB = VectorClock.Create(("A", 1));

        vA.Compare(vB).Should().Be(CausalRelation.After);
        (vA > vB).Should().BeTrue();
        (vA >= vB).Should().BeTrue();
        (vA < vB).Should().BeFalse();
        (vA <= vB).Should().BeFalse();
        (vA == vB).Should().BeFalse();
        vA.IsDominating(vB).Should().BeTrue();
        vA.IsDominatedBy(vB).Should().BeFalse();
    }

    [Fact]
    public void KnownAnswer_V4_Concurrent()
    {
        // V4: {A: 1, B: 0} vs {A: 0, B: 1} => Concurrent (VA || VB)
        var vA = VectorClock.Create(("A", 1), ("B", 0));
        var vB = VectorClock.Create(("A", 0), ("B", 1));

        vA.Compare(vB).Should().Be(CausalRelation.Concurrent);
        vB.Compare(vA).Should().Be(CausalRelation.Concurrent);
        vA.IsConcurrentWith(vB).Should().BeTrue();
        (vA < vB).Should().BeFalse();
        (vA > vB).Should().BeFalse();
        (vA <= vB).Should().BeFalse();
        (vA >= vB).Should().BeFalse();
        (vA == vB).Should().BeFalse();
        (vA != vB).Should().BeTrue();
    }

    [Fact]
    public void KnownAnswer_V5_Concurrent_ThreePeers()
    {
        // V5: {A: 3, B: 2, C: 1} vs {A: 2, B: 3, C: 1} => Concurrent (VA || VB)
        var vA = VectorClock.Create(("A", 3), ("B", 2), ("C", 1));
        var vB = VectorClock.Create(("A", 2), ("B", 3), ("C", 1));

        vA.Compare(vB).Should().Be(CausalRelation.Concurrent);
        vB.Compare(vA).Should().Be(CausalRelation.Concurrent);
        vA.IsConcurrentWith(vB).Should().BeTrue();
    }

    [Fact]
    public void KnownAnswer_V6_Before_ThreePeersWithSubset()
    {
        // V6: {A: 2, B: 2} vs {A: 3, B: 2, C: 1} => Before (VA < VB)
        var vA = VectorClock.Create(("A", 2), ("B", 2));
        var vB = VectorClock.Create(("A", 3), ("B", 2), ("C", 1));

        vA.Compare(vB).Should().Be(CausalRelation.Before);
        vB.Compare(vA).Should().Be(CausalRelation.After);
        (vA < vB).Should().BeTrue();
        (vB > vA).Should().BeTrue();
    }

    #endregion

    #region Edge Cases & Boundary Invariants

    [Fact]
    public void EmptyVector_IsOrigin_DominatedByAnyNonEmptyClock()
    {
        var empty = VectorClock.Empty;
        var clock = VectorClock.Create(("node-1", 1));

        empty.IsEmpty.Should().BeTrue();
        empty.Count.Should().Be(0);
        empty["node-1"].Should().Be(0);

        empty.Compare(clock).Should().Be(CausalRelation.Before);
        clock.Compare(empty).Should().Be(CausalRelation.After);
        (empty < clock).Should().BeTrue();
        (clock > empty).Should().BeTrue();

        empty.Compare(VectorClock.Empty).Should().Be(CausalRelation.Equal);
        (empty == VectorClock.Empty).Should().BeTrue();
    }

    [Fact]
    public void MissingKey_ImplicitlyReturnsZero()
    {
        var clock = VectorClock.Create(("node-a", 10));

        clock["node-a"].Should().Be(10);
        clock["node-b"].Should().Be(0);
        clock.ContainsKey("node-a").Should().BeTrue();
        clock.ContainsKey("node-b").Should().BeFalse();
    }

    [Fact]
    public void PeerId_IsCaseInsensitiveNormalized()
    {
        var v1 = VectorClock.Create(("laptop-alice", 5));
        var v2 = VectorClock.Create(("LAPTOP-ALICE", 5));

        v1.Compare(v2).Should().Be(CausalRelation.Equal);
        (v1 == v2).Should().BeTrue();
        v1.GetHashCode().Should().Be(v2.GetHashCode());

        v1["LAPTOP-ALICE"].Should().Be(5);
        v2["laptop-alice"].Should().Be(5);
    }

    [Fact]
    public void ZeroCounters_AreOmittedFromInternalStorage()
    {
        var clock = VectorClock.Create(("node-1", 0), ("node-2", 0));
        clock.IsEmpty.Should().BeTrue();
        clock.Count.Should().Be(0);
        clock.Should().BeSameAs(VectorClock.Empty);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void InvalidPeerId_ThrowsArgumentException(string? invalidPeer)
    {
        FluentActions.Invoking(() => VectorClock.Empty.Tick(invalidPeer!))
            .Should().Throw<ArgumentException>();

        FluentActions.Invoking(() => VectorClock.Create((invalidPeer!, 1UL)))
            .Should().Throw<ArgumentException>();
    }

    [Fact]
    public void DisjointPeers_AreAlwaysConcurrent()
    {
        var vA = VectorClock.Create(("node-A", 100));
        var vB = VectorClock.Create(("node-B", 1));

        vA.Compare(vB).Should().Be(CausalRelation.Concurrent);
        vB.Compare(vA).Should().Be(CausalRelation.Concurrent);
    }

    #endregion

    #region Tick & Merge Operations

    [Fact]
    public void Tick_IncrementsLocalPeerMonotonically()
    {
        var clock0 = VectorClock.Empty;
        var clock1 = clock0.Tick("node-1");
        var clock2 = clock1.Tick("node-1");

        clock0["node-1"].Should().Be(0);
        clock1["node-1"].Should().Be(1);
        clock2["node-1"].Should().Be(2);

        (clock0 < clock1).Should().BeTrue();
        (clock1 < clock2).Should().BeTrue();
    }

    [Fact]
    public void Merge_ComputesComponentWiseSupremum()
    {
        var v1 = VectorClock.Create(("A", 5), ("B", 2), ("C", 1));
        var v2 = VectorClock.Create(("A", 3), ("B", 7), ("D", 4));

        var sup = v1.Merge(v2);

        sup["A"].Should().Be(5);
        sup["B"].Should().Be(7);
        sup["C"].Should().Be(1);
        sup["D"].Should().Be(4);

        // Supremum dominates both inputs
        (v1 <= sup).Should().BeTrue();
        (v2 <= sup).Should().BeTrue();
    }

    [Fact]
    public void Merge_WithEmptyOrSelf_IsIdempotent()
    {
        var v = VectorClock.Create(("A", 2), ("B", 3));

        v.Merge(VectorClock.Empty).Should().BeSameAs(v);
        VectorClock.Empty.Merge(v).Should().BeSameAs(v);
        v.Merge(v).Should().BeSameAs(v);
    }

    #endregion

    #region Serialization & Roundtrips

    [Fact]
    public void JsonSerialization_RoundtripsDeterministically()
    {
        var clock = VectorClock.Create(("B", 4), ("A", 2), ("C", 9));
        string json = clock.ToJson();

        json.Should().Contain("\"A\":2");
        json.Should().Contain("\"B\":4");
        json.Should().Contain("\"C\":9");

        var deserialized = VectorClock.FromJson(json);
        (deserialized == clock).Should().BeTrue();
        deserialized.Compare(clock).Should().Be(CausalRelation.Equal);
    }

    [Fact]
    public void BinarySerialization_RoundtripsDeterministically()
    {
        var clock = VectorClock.Create(("laptop-alice", 123456789UL), ("server-us-east", 42UL));
        byte[] bytes = clock.ToByteArray();

        bytes.Length.Should().BeGreaterThan(0);

        var restored = VectorClock.FromByteArray(bytes);
        (restored == clock).Should().BeTrue();
        restored.Compare(clock).Should().Be(CausalRelation.Equal);
    }

    [Fact]
    public void EmptyClock_BinaryRoundtrip()
    {
        byte[] emptyBytes = VectorClock.Empty.ToByteArray();
        emptyBytes.Should().BeEmpty();

        var restored = VectorClock.FromByteArray(emptyBytes);
        (restored == VectorClock.Empty).Should().BeTrue();
        restored.IsEmpty.Should().BeTrue();
    }

    [Fact]
    public void ToString_FormatsSortedReadableRepresentation()
    {
        var clock = VectorClock.Create(("B", 2), ("A", 1));
        clock.ToString().Should().Be("{A: 1, B: 2}");

        VectorClock.Empty.ToString().Should().Be("{}");
    }

    #endregion
}
