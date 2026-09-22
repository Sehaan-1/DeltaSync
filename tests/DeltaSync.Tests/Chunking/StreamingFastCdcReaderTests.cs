using System.Security.Cryptography;
using System.Text;
using DeltaSync.Core.Chunking;
using Xunit;

namespace DeltaSync.Tests.Chunking;

public class StreamingFastCdcReaderTests
{
    private static readonly byte[] KnownVectorBytes = Encoding.ASCII.GetBytes(
        string.Concat(Enumerable.Repeat("DeltaSync-FastCDC-Verification-String-0123456789\n", 25_000)));

    [Fact]
    public void EmptyStream_ProducesEmptyManifestWithCanonicalRootHash()
    {
        using var emptyStream = new MemoryStream();
        var manifest = StreamingFastCdcReader.ReadManifest("empty.bin", emptyStream);

        Assert.Equal("empty.bin", manifest.RelativePath);
        Assert.Equal(0, manifest.FileSize);
        Assert.Equal(FileManifest.EmptyRootHash, manifest.RootHash);
        Assert.Empty(manifest.Chunks);

        emptyStream.Position = 0;
        var chunks = StreamingFastCdcReader.ReadChunks(emptyStream).ToList();
        Assert.Empty(chunks);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(500)]
    [InlineData(16_383)]
    [InlineData(16_384)]
    [InlineData(65_536)]
    public void MicroAndSmallStreams_MatchInMemoryManifestBitForBit(int length)
    {
        byte[] buffer = new byte[length];
        Random.Shared.NextBytes(buffer);

        var inMemoryManifest = ChunkFingerprinter.CreateManifest("micro.bin", buffer);

        using var stream = new MemoryStream(buffer);
        var streamedManifest = StreamingFastCdcReader.ReadManifest("micro.bin", stream);

        Assert.Equal(inMemoryManifest.FileSize, streamedManifest.FileSize);
        Assert.Equal(inMemoryManifest.RootHash, streamedManifest.RootHash);
        Assert.Equal(inMemoryManifest.Chunks.Count, streamedManifest.Chunks.Count);

        for (int i = 0; i < inMemoryManifest.Chunks.Count; i++)
        {
            Assert.Equal(inMemoryManifest.Chunks[i].Index, streamedManifest.Chunks[i].Index);
            Assert.Equal(inMemoryManifest.Chunks[i].Offset, streamedManifest.Chunks[i].Offset);
            Assert.Equal(inMemoryManifest.Chunks[i].Length, streamedManifest.Chunks[i].Length);
            Assert.Equal(inMemoryManifest.Chunks[i].HashHex, streamedManifest.Chunks[i].HashHex);
        }
    }

    [Fact]
    public void ReadManifest_KnownAnswerVector_MatchesInMemoryManifestExactly()
    {
        var inMemoryManifest = ChunkFingerprinter.CreateManifest("vector.bin", KnownVectorBytes);

        using var stream = new MemoryStream(KnownVectorBytes);
        var streamedManifest = StreamingFastCdcReader.ReadManifest("vector.bin", stream);

        Assert.Equal(inMemoryManifest.FileSize, streamedManifest.FileSize);
        Assert.Equal(inMemoryManifest.RootHash, streamedManifest.RootHash);
        Assert.Equal(inMemoryManifest.Chunks.Count, streamedManifest.Chunks.Count);

        for (int i = 0; i < inMemoryManifest.Chunks.Count; i++)
        {
            Assert.Equal(inMemoryManifest.Chunks[i].Index, streamedManifest.Chunks[i].Index);
            Assert.Equal(inMemoryManifest.Chunks[i].Offset, streamedManifest.Chunks[i].Offset);
            Assert.Equal(inMemoryManifest.Chunks[i].Length, streamedManifest.Chunks[i].Length);
            Assert.Equal(inMemoryManifest.Chunks[i].HashHex, streamedManifest.Chunks[i].HashHex);
        }
    }

    [Fact]
    public void ReadChunks_EmitsPayloadsMatchingStreamBitForBit()
    {
        using var stream = new MemoryStream(KnownVectorBytes);
        var chunks = StreamingFastCdcReader.ReadChunks(stream).ToList();

        using var assembledStream = new MemoryStream();
        long currentOffset = 0;

        for (int i = 0; i < chunks.Count; i++)
        {
            var chunk = chunks[i];
            Assert.Equal(i, chunk.Index);
            Assert.Equal(currentOffset, chunk.Offset);

            byte[] expectedHash = SHA256.HashData(chunk.Payload.Span);
            Assert.Equal(chunk.Descriptor.HashHex, Convert.ToHexString(expectedHash).ToLowerInvariant());

            assembledStream.Write(chunk.Payload.Span);
            currentOffset += chunk.Length;
        }

        Assert.Equal(KnownVectorBytes.Length, assembledStream.Length);
        Assert.True(assembledStream.ToArray().AsSpan().SequenceEqual(KnownVectorBytes));
    }

    [Fact]
    public async Task ReadManifestAsync_MatchesSynchronousReadManifest()
    {
        using var stream1 = new MemoryStream(KnownVectorBytes);
        using var stream2 = new MemoryStream(KnownVectorBytes);

        var syncManifest = StreamingFastCdcReader.ReadManifest("async.bin", stream1);
        var asyncManifest = await StreamingFastCdcReader.ReadManifestAsync("async.bin", stream2);

        Assert.Equal(syncManifest.FileSize, asyncManifest.FileSize);
        Assert.Equal(syncManifest.RootHash, asyncManifest.RootHash);
        Assert.Equal(syncManifest.Chunks.Count, asyncManifest.Chunks.Count);

        var asyncChunks = new List<ChunkData>();
        stream2.Position = 0;
        await foreach (var chunk in StreamingFastCdcReader.ReadChunksAsync(stream2))
        {
            asyncChunks.Add(chunk);
        }

        Assert.Equal(syncManifest.Chunks.Count, asyncChunks.Count);
    }

    [Fact]
    public async Task ReadChunksAsync_HonorsCancellationToken()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        using var stream = new MemoryStream(KnownVectorBytes);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in StreamingFastCdcReader.ReadChunksAsync(stream, cancellationToken: cts.Token))
            {
            }
        });
    }

    [Fact]
    public void SlidingBuffer_WithSmallBufferSize_HandlesLargeChunksGracefully()
    {
        // Even if custom bufferSize passed is smaller than 2 * MaxSize, capacity clamps to Math.Max(bufferSize, 2 * MaxSize)
        using var stream = new MemoryStream(KnownVectorBytes);
        var manifest = StreamingFastCdcReader.ReadManifest("clamped.bin", stream, bufferSize: 1024);

        var expectedManifest = ChunkFingerprinter.CreateManifest("clamped.bin", KnownVectorBytes);
        Assert.Equal(expectedManifest.RootHash, manifest.RootHash);
        Assert.Equal(expectedManifest.Chunks.Count, manifest.Chunks.Count);
    }

    [Fact]
    public void MemoryBoundedStreamReader_CanProcessMultiMegabyteStreamWithoutLoadingIntoMemory()
    {
        // Custom non-seekable repeating stream that simulates a 20MB file
        using var repeatingStream = new RepeatingPatternStream(length: 20 * 1024 * 1024);

        long memBefore = GC.GetAllocatedBytesForCurrentThread();
        var manifest = StreamingFastCdcReader.ReadManifest("large.bin", repeatingStream);
        long memAfter = GC.GetAllocatedBytesForCurrentThread();

        Assert.Equal(20 * 1024 * 1024, manifest.FileSize);
        Assert.True(manifest.Chunks.Count > 0);

        // Memory allocated during ReadManifest for a 20MB stream should be strictly bounded to < 2 MB
        long allocatedBytes = memAfter - memBefore;
        Assert.True(allocatedBytes < 2 * 1024 * 1024,
            $"Expected < 2MB allocated during ReadManifest, but measured {allocatedBytes / 1024.0:F1} KB.");
    }

    private sealed class RepeatingPatternStream : Stream
    {
        private readonly long _totalLength;
        private long _position;
        private static readonly byte[] Pattern = Encoding.ASCII.GetBytes("DeltaSync-FastCDC-Streaming-Bounded-Memory-Check-0123456789\n");

        public RepeatingPatternStream(long length) => _totalLength = length;

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _totalLength;
        public override long Position { get => _position; set => throw new NotSupportedException(); }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_position >= _totalLength) return 0;
            int toRead = (int)Math.Min(count, _totalLength - _position);
            for (int i = 0; i < toRead; i++)
            {
                buffer[offset + i] = Pattern[(_position + i) % Pattern.Length];
            }
            _position += toRead;
            return toRead;
        }

        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
