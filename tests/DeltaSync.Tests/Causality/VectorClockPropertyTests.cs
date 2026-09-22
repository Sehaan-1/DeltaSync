using System.Diagnostics;
using DeltaSync.Core.Causality;
using FluentAssertions;
using FsCheck;
using FsCheck.Xunit;
using Xunit;

namespace DeltaSync.Tests.Causality;

public class VectorClockPropertyTests
{
    private static readonly string[] PeerPool = ["node-A", "node-B", "node-C", "node-D", "node-E", "node-F"];

    public VectorClockPropertyTests()
    {
    }

    private static Gen<VectorClock> Generator()
    {
        var entryGen = from peer in Gen.Elements(PeerPool)
                       from counter in Gen.Choose(0, 100)
                       select (peer, (ulong)counter);

        return from count in Gen.Choose(0, 5)
               from entries in Gen.ListOf(count, entryGen)
               select VectorClock.Create(entries.ToArray());
    }

    [Property(MaxTest = 10000)]
    public Property Reflexivity_EveryClock_IsEqualToAndDominatesItself()
    {
        return Prop.ForAll(Generator().ToArbitrary(), clock =>
        {
            var self = clock;
            bool eq = clock.Compare(self) == CausalRelation.Equal;
            bool dominates = clock.IsDominating(self);
            bool dominated = clock.IsDominatedBy(self);
            bool opLe = clock <= self;
            bool opGe = clock >= self;
            bool opEq = clock == self;

            return (eq && dominates && dominated && opLe && opGe && opEq).ToProperty();
        });
    }

    [Property(MaxTest = 10000)]
    public Property Antisymmetry_TwoClocksDominatingEachOther_AreIdentical()
    {
        var pairGen = from v1 in Generator()
                      from v2 in Generator()
                      select (v1, v2);

        return Prop.ForAll(pairGen.ToArbitrary(), pair =>
        {
            var (v1, v2) = pair;
            bool v1DomV2 = v1 <= v2;
            bool v2DomV1 = v2 <= v1;

            if (v1DomV2 && v2DomV1)
            {
                return (v1 == v2 && v1.Compare(v2) == CausalRelation.Equal).ToProperty();
            }

            return true.ToProperty();
        });
    }

    [Property(MaxTest = 10000)]
    public Property Transitivity_OrderedChain_MaintainsOrder()
    {
        var tripletGen = from v1 in Generator()
                         from v2 in Generator()
                         from v3 in Generator()
                         select (v1, v2, v3);

        return Prop.ForAll(tripletGen.ToArbitrary(), triplet =>
        {
            var (v1, v2, v3) = triplet;

            if (v1 <= v2 && v2 <= v3)
            {
                return (v1 <= v3).ToProperty();
            }

            return true.ToProperty();
        });
    }

    [Property(MaxTest = 10000)]
    public Property ConcurrencySymmetry_IfConcurrent_ReverseIsConcurrent()
    {
        var pairGen = from v1 in Generator()
                      from v2 in Generator()
                      select (v1, v2);

        return Prop.ForAll(pairGen.ToArbitrary(), pair =>
        {
            var (v1, v2) = pair;
            var rel1 = v1.Compare(v2);
            var rel2 = v2.Compare(v1);

            if (rel1 == CausalRelation.Concurrent)
            {
                return (rel2 == CausalRelation.Concurrent && v1.IsConcurrentWith(v2) && v2.IsConcurrentWith(v1)).ToProperty();
            }

            if (rel1 == CausalRelation.Before)
            {
                return (rel2 == CausalRelation.After).ToProperty();
            }

            if (rel1 == CausalRelation.After)
            {
                return (rel2 == CausalRelation.Before).ToProperty();
            }

            return (rel2 == CausalRelation.Equal).ToProperty();
        });
    }

    [Property(MaxTest = 10000)]
    public Property SupremumDominance_Merge_DominatesBothInputs()
    {
        var pairGen = from v1 in Generator()
                      from v2 in Generator()
                      select (v1, v2);

        return Prop.ForAll(pairGen.ToArbitrary(), pair =>
        {
            var (v1, v2) = pair;
            var sup = v1.Merge(v2);

            bool dominates1 = v1 <= sup;
            bool dominates2 = v2 <= sup;

            return (dominates1 && dominates2).ToProperty();
        });
    }

    [Property(MaxTest = 10000)]
    public Property SupremumLeastUpperBound_SupremumIsSmallestDominatingClock()
    {
        var tripletGen = from v1 in Generator()
                         from v2 in Generator()
                         from w in Generator()
                         select (v1, v2, w);

        return Prop.ForAll(tripletGen.ToArbitrary(), triplet =>
        {
            var (v1, v2, w) = triplet;

            // If W dominates both V1 and V2, then supremum(V1, V2) must also be dominated by W
            if (v1 <= w && v2 <= w)
            {
                var sup = v1.Merge(v2);
                return (sup <= w).ToProperty();
            }

            return true.ToProperty();
        });
    }

    [Property(MaxTest = 10000)]
    public Property MonotonicProgress_Tick_AlwaysStrictlyDominates()
    {
        var tickGen = from clock in Generator()
                      from peer in Gen.Elements(PeerPool)
                      select (clock, peer);

        return Prop.ForAll(tickGen.ToArbitrary(), tuple =>
        {
            var (clock, peer) = tuple;
            var ticked = clock.Tick(peer);

            bool strictlyGreater = clock < ticked;
            bool counterIncremented = ticked[peer] == clock[peer] + 1UL;

            return (strictlyGreater && counterIncremented).ToProperty();
        });
    }

    [Fact]
    public void Benchmark_ComparisonAndMerge_NanosecondLatency()
    {
        // Spec §5 target: ~45 ns per comparison, ~60 ns per supremum merge
        var v1 = VectorClock.Create(("node-A", 10), ("node-B", 20), ("node-C", 30), ("node-D", 40), ("node-E", 50));
        var v2 = VectorClock.Create(("node-A", 10), ("node-B", 25), ("node-C", 30), ("node-D", 35), ("node-E", 50));

        // Warmup
        for (int i = 0; i < 20_000; i++)
        {
            _ = v1.Compare(v2);
            _ = v1.Merge(v2);
        }

        const int iterations = 200_000;

        // Measure Compare
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < iterations; i++)
        {
            _ = v1.Compare(v2);
        }
        sw.Stop();
        double nsPerCompare = (sw.Elapsed.TotalMilliseconds * 1_000_000.0) / iterations;

        // Measure Merge
        sw.Restart();
        for (int i = 0; i < iterations; i++)
        {
            _ = v1.Merge(v2);
        }
        sw.Stop();
        double nsPerMerge = (sw.Elapsed.TotalMilliseconds * 1_000_000.0) / iterations;

        Console.WriteLine($"VectorClock Benchmark ({iterations} iterations, 5 nodes):");
        Console.WriteLine($"  Compare: {nsPerCompare:F2} ns/op");
        Console.WriteLine($"  Merge:   {nsPerMerge:F2} ns/op");

        // Assert nanosecond-scale performance (well under 500 ns)
        nsPerCompare.Should().BeLessThan(500.0);
        nsPerMerge.Should().BeLessThan(1000.0);
    }
}
