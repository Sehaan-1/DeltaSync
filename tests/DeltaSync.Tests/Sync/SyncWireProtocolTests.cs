using System.Security.Cryptography;
using System.Text;
using DeltaSync.Core.Causality;
using DeltaSync.Core.Chunking;
using DeltaSync.Core.Models;
using DeltaSync.Core.Storage;
using DeltaSync.Core.Sync;
using DeltaSync.Core.Sync.Wire;
using DeltaSync.Network;
using FluentAssertions;
using Xunit;

namespace DeltaSync.Tests.Sync;

public class SyncWireProtocolTests : IAsyncDisposable
{
    private readonly Guid _clusterId = Guid.NewGuid();
    private readonly List<IAsyncDisposable> _disposables = new();
    private readonly string _tempDir;

    public SyncWireProtocolTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"deltasync_wire_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var d in _disposables)
        {
            await d.DisposeAsync();
        }

        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, true); } catch { }
        }
    }

    private (SyncWireProtocol LocalProto, SyncWireProtocol RemoteProto, InMemoryTransportChannel LocalChannel, InMemoryTransportChannel RemoteChannel)
        CreateProtocolPair(ISqliteStateStore localStore, ISqliteStateStore remoteStore, ILocalChunkProvider? localChunks = null, ILocalChunkProvider? remoteChunks = null)
    {
        var (localChannel, remoteChannel) = InMemoryTransportChannel.CreateConnectedPair("peer-local", "peer-remote", _clusterId);
        _disposables.Add(localChannel);
        _disposables.Add(remoteChannel);

        var localProto = new SyncWireProtocol(localStore, localChunks);
        var remoteProto = new SyncWireProtocol(remoteStore, remoteChunks);
        _disposables.Add(localProto);
        _disposables.Add(remoteProto);

        localProto.AttachChannel(localChannel);
        remoteProto.AttachChannel(remoteChannel);

        return (localProto, remoteProto, localChannel, remoteChannel);
    }

    private static (FileMetadata Meta, IReadOnlyList<ChunkDescriptor> Chunks, byte[] Data) CreateTestFile(
        string relativePath,
        string content,
        string peerId = "peer-1")
    {
        byte[] data = Encoding.UTF8.GetBytes(content);
        var manifest = ChunkFingerprinter.CreateManifest(relativePath, data);
        var clock = VectorClock.Empty.Tick(peerId);
        var meta = new FileMetadata(manifest.RelativePath, manifest.FileSize, manifest.RootHash, DateTimeOffset.UtcNow, clock);
        return (meta, manifest.Chunks, data);
    }

    [Fact]
    public async Task ProbeRootEqualityAsync_WhenRootsMatch_ReturnsTrue()
    {
        using var localStore = new MemoryStateStore();
        using var remoteStore = new MemoryStateStore();

        var (meta, chunks, _) = CreateTestFile("hello.txt", "Hello DeltaSync World");
        await localStore.UpsertFileAsync(meta, chunks);
        await remoteStore.UpsertFileAsync(meta, chunks);

        var (localProto, _, localChannel, _) = CreateProtocolPair(localStore, remoteStore);

        bool match = await localProto.ProbeRootEqualityAsync(localChannel);
        match.Should().BeTrue("Identical files on both peers must produce matching Merkle roots");
    }

    [Fact]
    public async Task ProbeRootEqualityAsync_WhenRootsDiverge_ReturnsFalse()
    {
        using var localStore = new MemoryStateStore();
        using var remoteStore = new MemoryStateStore();

        var (metaA, chunksA, _) = CreateTestFile("hello.txt", "Version A");
        var (metaB, chunksB, _) = CreateTestFile("hello.txt", "Version B");
        await localStore.UpsertFileAsync(metaA, chunksA);
        await remoteStore.UpsertFileAsync(metaB, chunksB);

        var (localProto, _, localChannel, _) = CreateProtocolPair(localStore, remoteStore);

        bool match = await localProto.ProbeRootEqualityAsync(localChannel);
        match.Should().BeFalse("Divergent files must produce divergent Merkle roots");
    }

    [Fact]
    public async Task ReconcilePrefixAsync_WhenNestedSubtreeDiverges_PrunesMatchingSubtrees()
    {
        using var localStore = new MemoryStateStore();
        using var remoteStore = new MemoryStateStore();

        // Common files in src/ and assets/
        var (shared1, chunks1, _) = CreateTestFile("src/App.cs", "class App {}");
        var (shared2, chunks2, _) = CreateTestFile("assets/icon.png", "PNG_DATA");
        await localStore.UpsertFileAsync(shared1, chunks1);
        await remoteStore.UpsertFileAsync(shared1, chunks1);
        await localStore.UpsertFileAsync(shared2, chunks2);
        await remoteStore.UpsertFileAsync(shared2, chunks2);

        // Divergent file in docs/arch/
        var (localDoc, docChunksLocal, _) = CreateTestFile("docs/arch/spec.md", "Specification v1");
        var (remoteDoc, docChunksRemote, _) = CreateTestFile("docs/arch/spec.md", "Specification v2 (Modified)");
        await localStore.UpsertFileAsync(localDoc, docChunksLocal);
        await remoteStore.UpsertFileAsync(remoteDoc, docChunksRemote);

        var (localProto, _, localChannel, _) = CreateProtocolPair(localStore, remoteStore);

        var divergent = await localProto.ReconcilePrefixAsync(localChannel, "");

        divergent.Should().HaveCount(1);
        divergent[0].RelativePath.Should().Be("docs/arch/spec.md");
        divergent[0].IsContentModified.Should().BeTrue();
        divergent[0].RemoteRootHash.Should().Be(remoteDoc.RootHash);
        divergent[0].LocalRootHash.Should().Be(localDoc.RootHash);
    }

    [Fact]
    public async Task FetchManifestAsync_ReturnsRemoteChunksAndVectorClock()
    {
        using var localStore = new MemoryStateStore();
        using var remoteStore = new MemoryStateStore();

        var (meta, chunks, _) = CreateTestFile("src/utils.cs", "public static void Foo() {}", "node-remote");
        await remoteStore.UpsertFileAsync(meta, chunks);

        var (localProto, _, localChannel, _) = CreateProtocolPair(localStore, remoteStore);

        var manifestResp = await localProto.FetchManifestAsync(localChannel, "src/utils.cs");

        manifestResp.RelativePath.Should().Be("src/utils.cs");
        manifestResp.ContentHash.Should().Be(meta.RootHash);
        manifestResp.Clock["node-remote"].Should().Be(1);
        manifestResp.Chunks.Should().HaveCount(chunks.Count);
    }

    [Fact]
    public async Task FetchChunkAsync_WhenValid_ReturnsPayload()
    {
        using var localStore = new MemoryStateStore();
        using var remoteStore = new MemoryStateStore();
        var remoteChunkSource = new MemoryChunkProvider();

        byte[] payload = Encoding.UTF8.GetBytes("Special Chunk Payload 12345");
        byte[] hash = SHA256.HashData(payload);
        string hashHex = Convert.ToHexString(hash).ToLowerInvariant();
        remoteChunkSource.AddChunk(hashHex, payload);

        var (localProto, _, localChannel, _) = CreateProtocolPair(localStore, remoteStore, remoteChunks: remoteChunkSource);

        var fetched = await localProto.FetchChunkAsync(localChannel, hashHex);

        fetched.ToArray().Should().Equal(payload);
    }

    [Fact]
    public async Task FetchChunkAsync_WhenHashCorrupted_ThrowsChunkIntegrityException()
    {
        using var localStore = new MemoryStateStore();
        using var remoteStore = new MemoryStateStore();
        var remoteChunkSource = new MemoryChunkProvider();

        byte[] realPayload = Encoding.UTF8.GetBytes("Real Payload");
        byte[] corruptedPayload = Encoding.UTF8.GetBytes("Corrupted Payload!!");
        byte[] expectedHash = SHA256.HashData(realPayload);
        string expectedHex = Convert.ToHexString(expectedHash).ToLowerInvariant();

        // Inject corrupted payload for expected hash
        remoteChunkSource.AddChunk(expectedHex, corruptedPayload);

        var (localProto, _, localChannel, _) = CreateProtocolPair(localStore, remoteStore, remoteChunks: remoteChunkSource);

        var act = async () => await localProto.FetchChunkAsync(localChannel, expectedHex);

        await act.Should().ThrowAsync<ChunkIntegrityException>();
    }

    [Fact]
    public async Task FetchChunksPipelinedAsync_FetchesAllRequestedChunksConcurrently()
    {
        using var localStore = new MemoryStateStore();
        using var remoteStore = new MemoryStateStore();
        var remoteChunks = new MemoryChunkProvider();

        var hashes = new List<string>();
        for (int i = 0; i < 30; i++)
        {
            byte[] data = Encoding.UTF8.GetBytes($"Pipelined chunk content #{i}");
            byte[] hash = SHA256.HashData(data);
            string hex = Convert.ToHexString(hash).ToLowerInvariant();
            hashes.Add(hex);
            remoteChunks.AddChunk(hex, data);
        }

        var (localProto, _, localChannel, _) = CreateProtocolPair(localStore, remoteStore, remoteChunks: remoteChunks);

        var results = await localProto.FetchChunksPipelinedAsync(localChannel, hashes, windowSize: 16);

        results.Should().HaveCount(30);
        foreach (var hash in hashes)
        {
            results.Should().ContainKey(hash);
        }
    }

    [Fact]
    public async Task FetchAndReconstructFileAsync_StitchesLocalAndRemoteChunksSuccessfully()
    {
        using var localStore = new MemoryStateStore();
        using var remoteStore = new MemoryStateStore();

        // Create base content (128 KB)
        byte[] baseData = new byte[128 * 1024];
        new Random(42).NextBytes(baseData);
        var baseManifest = ChunkFingerprinter.CreateManifest("data.bin", baseData);

        // Edit small portion at the end
        byte[] modifiedData = (byte[])baseData.Clone();
        modifiedData[^10] = (byte)(modifiedData[^10] ^ 0xFF);
        var modifiedManifest = ChunkFingerprinter.CreateManifest("data.bin", modifiedData);

        // Local peer has baseData chunks
        var localChunkProvider = new MemoryChunkProvider();
        foreach (var c in baseManifest.Chunks)
        {
            byte[] cBytes = baseData.AsSpan((int)c.Offset, c.Length).ToArray();
            localChunkProvider.AddChunk(c.HashHex, cBytes);
        }

        // Remote peer has modifiedData chunks
        var remoteChunkProvider = new MemoryChunkProvider();
        foreach (var c in modifiedManifest.Chunks)
        {
            byte[] cBytes = modifiedData.AsSpan((int)c.Offset, c.Length).ToArray();
            remoteChunkProvider.AddChunk(c.HashHex, cBytes);
        }

        var (localProto, _, localChannel, _) = CreateProtocolPair(
            localStore, remoteStore,
            localChunks: localChunkProvider,
            remoteChunks: remoteChunkProvider);

        var manifestResp = FileManifestResponse.FromFileManifest(modifiedManifest, VectorClock.Empty.Tick("remote"));
        string destPath = Path.Combine(_tempDir, "reconstructed.bin");

        var result = await localProto.FetchAndReconstructFileAsync(
            localChannel,
            manifestResp,
            destPath,
            tempDirectory: _tempDir);

        result.TotalBytes.Should().Be(modifiedData.Length);
        result.ReusedChunks.Should().BeGreaterThan(0, "Identical chunks should be reused from local chunk provider");
        result.TransmittedChunks.Should().BeLessThan(modifiedManifest.Chunks.Count, "Only modified chunks should be transmitted over the wire");
        result.BandwidthSavingsRatio.Should().BeGreaterThan(0.5);

        byte[] onDisk = await File.ReadAllBytesAsync(destPath);
        onDisk.Should().Equal(modifiedData, "Reconstructed file must be bit-for-bit identical to modified content");
    }
}
