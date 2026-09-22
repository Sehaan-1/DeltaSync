using System.Security.Cryptography;
using DeltaSync.Core.Chunking;
using Xunit;

namespace DeltaSync.Tests.Chunking;

public class ChunkProviderTests
{
    [Fact]
    public void ChunkIntegrityException_InitializesWithCorrectFields()
    {
        var ex = new ChunkIntegrityException(3, "aabbcc", "ddeeff");
        Assert.Equal(3, ex.ChunkIndex);
        Assert.Equal("aabbcc", ex.ExpectedHash);
        Assert.Equal("ddeeff", ex.ActualHash);
        Assert.Contains("index 3", ex.Message);
        Assert.Contains("aabbcc", ex.Message);
        Assert.Contains("ddeeff", ex.Message);

        var rootEx = ChunkIntegrityException.RootHashMismatch("112233", "445566");
        Assert.Null(rootEx.ChunkIndex);
        Assert.Equal("112233", rootEx.ExpectedHash);
        Assert.Equal("445566", rootEx.ActualHash);
        Assert.Contains("112233", rootEx.Message);
    }

    [Fact]
    public async Task MemoryChunkProvider_RetrievesChunks_CaseInsensitive()
    {
        var provider = new MemoryChunkProvider();
        byte[] data = [1, 2, 3, 4, 5];
        byte[] hash = SHA256.HashData(data);
        string hashHex = Convert.ToHexString(hash);

        provider.AddChunk(hashHex, data);

        // Synchronous check (lowercase & uppercase)
        Assert.True(provider.TryGetChunk(hashHex.ToLowerInvariant(), out var slice1));
        Assert.True(slice1.Span.SequenceEqual(data));

        Assert.True(provider.TryGetChunk(hashHex.ToUpperInvariant(), out var slice2));
        Assert.True(slice2.Span.SequenceEqual(data));

        // Missing hash
        Assert.False(provider.TryGetChunk("00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff", out _));

        // Async check
        var asyncResult = await provider.GetChunkAsync(hashHex);
        Assert.NotNull(asyncResult);
        Assert.True(asyncResult.Value.Span.SequenceEqual(data));

        var missingAsync = await provider.GetChunkAsync("missing");
        Assert.Null(missingAsync);
    }

    [Fact]
    public void FileChunkProvider_ReadsChunksFromDiskAccurately()
    {
        string tempDir = Path.Combine(Path.GetTempPath(), "deltasync-test-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);
        string filePath = Path.Combine(tempDir, "sample.bin");

        try
        {
            byte[] fileData = new byte[100_000];
            Random.Shared.NextBytes(fileData);
            File.WriteAllBytes(filePath, fileData);

            var manifest = ChunkFingerprinter.CreateManifest("sample.bin", fileData);
            using var fileProvider = new FileChunkProvider(filePath, manifest);

            foreach (var chunk in manifest.Chunks)
            {
                Assert.True(fileProvider.TryGetChunk(chunk.HashHex, out var chunkBytes));
                Assert.Equal(chunk.Length, chunkBytes.Length);

                byte[] expectedSlice = fileData.AsSpan((int)chunk.Offset, chunk.Length).ToArray();
                Assert.True(chunkBytes.Span.SequenceEqual(expectedSlice));
            }

            Assert.False(fileProvider.TryGetChunk("0000000000000000000000000000000000000000000000000000000000000000", out _));
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, recursive: true);
            }
        }
    }

    [Fact]
    public async Task MemoryAndDelegateRemoteChunkSource_WorkCorrectly()
    {
        var memSource = new MemoryRemoteChunkSource();
        byte[] payload = [10, 20, 30];
        byte[] hash = SHA256.HashData(payload);
        var descriptor = new ChunkDescriptor(0, 0, payload.Length, hash);

        memSource.AddChunk(descriptor.HashHex, payload);

        var retrieved = await memSource.FetchChunkAsync(descriptor);
        Assert.True(retrieved.Span.SequenceEqual(payload));

        var missingDesc = new ChunkDescriptor(1, 3, 4, SHA256.HashData([9, 9, 9, 9]));
        await Assert.ThrowsAsync<KeyNotFoundException>(() => memSource.FetchChunkAsync(missingDesc).AsTask());

        var delegateSource = new DelegateRemoteChunkSource((chunk, ct) => ValueTask.FromResult((ReadOnlyMemory<byte>)payload));
        var fromDelegate = await delegateSource.FetchChunkAsync(descriptor);
        Assert.True(fromDelegate.Span.SequenceEqual(payload));
    }
}
