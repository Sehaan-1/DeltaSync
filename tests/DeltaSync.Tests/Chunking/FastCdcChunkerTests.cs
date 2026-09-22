using System.Diagnostics;
using DeltaSync.Core.Chunking;
using FluentAssertions;
using FsCheck;
using FsCheck.Xunit;
using Xunit;
using Xunit.Abstractions;
using Random = System.Random;

namespace DeltaSync.Tests.Chunking;

public class FastCdcChunkerTests
{
    public FastCdcChunkerTests()
    {
    }
    [Fact]
    public void EmptyBuffer_YieldsZeroChunks()
    {
        var empty = ReadOnlyMemory<byte>.Empty;
        var chunks = FastCdcChunker.Chunk(empty);

        chunks.Should().BeEmpty();

        int enumCount = 0;
        foreach (var slice in FastCdcChunker.EnumerateChunks(empty.Span))
        {
            enumCount++;
        }
        enumCount.Should().Be(0);

        FastCdcChunker.NextChunk(ReadOnlySpan<byte>.Empty).Should().Be(0);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(100)]
    [InlineData(1024)]
    [InlineData(16_383)]
    public void BufferSmallerThanMinSize_YieldsSingleChunk(int length)
    {
        var buffer = new byte[length];
        Array.Fill(buffer, (byte)0x42);

        var chunks = FastCdcChunker.Chunk(buffer);

        chunks.Should().HaveCount(1);
        chunks[0].Offset.Should().Be(0);
        chunks[0].Length.Should().Be(length);

        FastCdcChunker.NextChunk(buffer).Should().Be(length);
    }

    [Fact]
    public void BufferEqualToMinSize_YieldsSingleChunk()
    {
        int minSize = FastCdcConfig.Default.MinSize;
        var buffer = new byte[minSize];
        Array.Fill(buffer, (byte)0xAA);

        var chunks = FastCdcChunker.Chunk(buffer);

        chunks.Should().HaveCount(1);
        chunks[0].Offset.Should().Be(0);
        chunks[0].Length.Should().Be(minSize);

        FastCdcChunker.NextChunk(buffer).Should().Be(minSize);
    }

    [Fact]
    public void RepetitiveZeros_EnforcesMaxSizeClamp()
    {
        // 1 MB of uniform zero bytes
        int size = 1024 * 1024;
        var buffer = new byte[size];

        var chunks = FastCdcChunker.Chunk(buffer);

        chunks.Should().NotBeEmpty();
        long totalLength = 0;

        for (int i = 0; i < chunks.Count; i++)
        {
            var chunk = chunks[i];
            chunk.Length.Should().BeLessThanOrEqualTo(FastCdcConfig.Default.MaxSize);
            if (i < chunks.Count - 1)
            {
                chunk.Length.Should().BeGreaterThanOrEqualTo(FastCdcConfig.Default.MinSize);
            }
            else
            {
                chunk.Length.Should().BeGreaterThan(0);
            }
            totalLength += chunk.Length;
        }

        totalLength.Should().Be(size);
    }

    [Fact]
    public void Deterministic_SameInput_ProducesIdenticalPartition()
    {
        var random = new Random(12345);
        var buffer = new byte[200_000];
        random.NextBytes(buffer);

        var run1 = FastCdcChunker.Chunk(buffer);
        var run2 = FastCdcChunker.Chunk(buffer);

        run1.Should().Equal(run2);
    }

    [Fact]
    public void Config_ThrowsOnInvalidParameters()
    {
        // minSize < 1
        FluentActions.Invoking(() => new FastCdcConfig(minSize: 0))
            .Should().Throw<ArgumentOutOfRangeException>();

        // minSize >= targetSize
        FluentActions.Invoking(() => new FastCdcConfig(minSize: 64_000, targetSize: 64_000))
            .Should().Throw<ArgumentOutOfRangeException>();

        // targetSize >= maxSize
        FluentActions.Invoking(() => new FastCdcConfig(minSize: 16_000, targetSize: 128_000, maxSize: 128_000))
            .Should().Throw<ArgumentOutOfRangeException>();

        // normalizationLevel < 1
        FluentActions.Invoking(() => new FastCdcConfig(normalizationLevel: 0))
            .Should().Throw<ArgumentOutOfRangeException>();

        // normalizationLevel > 4
        FluentActions.Invoking(() => new FastCdcConfig(normalizationLevel: 5))
            .Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void LargeBuffer_10MB_SatisfiesAllInvariants()
    {
        // Test a full 10 MB buffer
        int size = 10 * 1024 * 1024;
        var buffer = new byte[size];
        var rng = new Random(42);
        rng.NextBytes(buffer);

        var chunks = FastCdcChunker.Chunk(buffer);

        chunks.Should().NotBeEmpty();
        long sum = 0;
        for (int i = 0; i < chunks.Count; i++)
        {
            var chunk = chunks[i];
            chunk.Offset.Should().Be(sum);
            if (i < chunks.Count - 1)
            {
                chunk.Length.Should().BeInRange(FastCdcConfig.Default.MinSize, FastCdcConfig.Default.MaxSize);
            }
            else
            {
                chunk.Length.Should().BeInRange(1, FastCdcConfig.Default.MaxSize);
            }
            sum += chunk.Length;
        }

        sum.Should().Be(size);
    }

    [Property(MaxTest = 1000)]
    public Property FsCheck_RandomPayloads_SatisfySizeBoundsAndExactSum()
    {
        // Generate payload sizes with realistic distribution:
        // - 10% empty/micro: [0, 100]
        // - 20% sub-minimum: [101, 16_384]
        // - 30% single-to-dual chunk: [16_385, 131_072]
        // - 30% multi-chunk: [131_073, 600_000]
        // - 10% large: [600_001, 1_500_000]
        var sizeGen = Gen.Frequency(
            Tuple.Create(10, Gen.Choose(0, 100)),
            Tuple.Create(20, Gen.Choose(101, 16_384)),
            Tuple.Create(30, Gen.Choose(16_385, 131_072)),
            Tuple.Create(30, Gen.Choose(131_073, 600_000)),
            Tuple.Create(10, Gen.Choose(600_001, 1_500_000))
        );

        var payloadGen = sizeGen.Select(size =>
        {
            var data = new byte[size];
            Random.Shared.NextBytes(data);
            return data;
        });

        return Prop.ForAll(payloadGen.ToArbitrary(), buffer =>
        {
            var chunks = FastCdcChunker.Chunk(buffer);

            if (buffer.Length == 0)
            {
                return (chunks.Count == 0).ToProperty();
            }

            long totalBytes = 0;
            int min = FastCdcConfig.Default.MinSize;
            int max = FastCdcConfig.Default.MaxSize;

            for (int i = 0; i < chunks.Count; i++)
            {
                var chunk = chunks[i];

                if (chunk.Offset != totalBytes)
                {
                    return false.Label($"Chunk {i} offset {chunk.Offset} != expected {totalBytes}");
                }

                if (i < chunks.Count - 1)
                {
                    if (chunk.Length < min || chunk.Length > max)
                    {
                        return false.Label($"Non-terminal chunk {i} size {chunk.Length} outside [{min}, {max}]");
                    }
                }
                else
                {
                    if (chunk.Length < 1 || chunk.Length > max)
                    {
                        return false.Label($"Terminal chunk {i} size {chunk.Length} outside [1, {max}]");
                    }
                }

                totalBytes += chunk.Length;
            }

            // Also verify zero-allocation enumerator equivalence
            int enumCount = 0;
            long enumTotal = 0;
            foreach (var slice in FastCdcChunker.EnumerateChunks(buffer))
            {
                if (slice != chunks[enumCount])
                {
                    return false.Label($"Enumerator chunk {enumCount} != list chunk");
                }
                enumCount++;
                enumTotal += slice.Length;
            }

            return (totalBytes == buffer.Length && enumCount == chunks.Count && enumTotal == buffer.Length).ToProperty();
        });
    }

    [Fact]
    public void Benchmark_ThroughputExceeds400MBps_ZeroAllocations()
    {
        // 50 MB synthetic buffer as specified in Ticket #9 check
        int size = 50 * 1024 * 1024;
        var buffer = new byte[size];
        var rng = new Random(98765);
        rng.NextBytes(buffer);

        // Warm up JIT
        int warmupCount = 0;
        foreach (var _ in FastCdcChunker.EnumerateChunks(buffer.AsSpan(0, 1024 * 1024)))
        {
            warmupCount++;
        }
        warmupCount.Should().BeGreaterThan(0);

        // Allocation measurement & Timing
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        long startTimestamp = Stopwatch.GetTimestamp();
        long allocBefore = GC.GetAllocatedBytesForCurrentThread();

        int chunkCount = 0;
        long totalBytes = 0;

        foreach (var slice in FastCdcChunker.EnumerateChunks(buffer))
        {
            chunkCount++;
            totalBytes += slice.Length;
        }

        long allocAfter = GC.GetAllocatedBytesForCurrentThread();
        long endTimestamp = Stopwatch.GetTimestamp();

        // 1. Verify exact reconstruction
        totalBytes.Should().Be(size);
        chunkCount.Should().BeGreaterThan(0);

        // 2. Verify ZERO managed heap allocations during boundary scan
        long allocatedBytes = allocAfter - allocBefore;
        allocatedBytes.Should().Be(0, "FastCdcSpanEnumerator must perform zero heap allocations in the hot scanning loop");

        // 3. Verify scan throughput >= 400 MB/s
        var elapsed = Stopwatch.GetElapsedTime(startTimestamp, endTimestamp);
        double seconds = elapsed.TotalSeconds;
        double mb = size / (1024.0 * 1024.0);
        double mbPerSec = mb / seconds;

        // Output to diagnostic and test output
        string summary = $"FastCDC 50MB Scan: {mbPerSec:F2} MB/s in {elapsed.TotalMilliseconds:F1} ms ({chunkCount} chunks, {allocatedBytes} bytes allocated)";
        Console.WriteLine(summary);
        Trace.WriteLine(summary);

        mbPerSec.Should().BeGreaterThanOrEqualTo(400.0, $"FastCDC boundary scanner must achieve at least 400 MB/s (measured: {mbPerSec:F2} MB/s)");
    }
}
