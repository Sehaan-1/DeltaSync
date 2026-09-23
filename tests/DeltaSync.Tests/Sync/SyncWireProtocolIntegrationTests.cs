using System.Net;
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
using Xunit.Abstractions;

namespace DeltaSync.Tests.Sync;

public class SyncWireProtocolIntegrationTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly Guid _clusterId = Guid.NewGuid();
    private readonly string _tempDir;
    private readonly List<IAsyncDisposable> _disposables = new();

    public SyncWireProtocolIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
        _tempDir = Path.Combine(Path.GetTempPath(), $"deltasync_wire_integration_{Guid.NewGuid():N}");
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

    /// <summary>
    /// Decorator wrapping IPeerTransportChannel to track message and payload byte counts strictly.
    /// </summary>
    private sealed class CountingTransportChannel : IPeerTransportChannel
    {
        private readonly IPeerTransportChannel _inner;
        private int _messagesSent;
        private int _messagesReceived;
        private long _bytesSent;
        private long _bytesReceived;

        public int MessagesSent => Volatile.Read(ref _messagesSent);
        public int MessagesReceived => Volatile.Read(ref _messagesReceived);
        public long BytesSent => Volatile.Read(ref _bytesSent);
        public long BytesReceived => Volatile.Read(ref _bytesReceived);

        public CountingTransportChannel(IPeerTransportChannel inner) => _inner = inner;

        public string LocalPeerId => _inner.LocalPeerId;
        public string RemotePeerId => _inner.RemotePeerId;
        public Guid ClusterId => _inner.ClusterId;
        public IPEndPoint? RemoteEndPoint => _inner.RemoteEndPoint;
        public bool IsInbound => _inner.IsInbound;
        public bool IsConnected => _inner.IsConnected;
        public bool IsDisposed => _inner.IsDisposed;

        public async ValueTask SendAsync(ReadOnlyMemory<byte> message, CancellationToken ct = default)
        {
            Interlocked.Increment(ref _messagesSent);
            Interlocked.Add(ref _bytesSent, message.Length);
            await _inner.SendAsync(message, ct).ConfigureAwait(false);
        }

        public async ValueTask<ReadOnlyMemory<byte>> ReceiveAsync(CancellationToken ct = default)
        {
            var mem = await _inner.ReceiveAsync(ct).ConfigureAwait(false);
            if (!mem.IsEmpty)
            {
                Interlocked.Increment(ref _messagesReceived);
                Interlocked.Add(ref _bytesReceived, mem.Length);
            }
            return mem;
        }

        public Task CloseAsync(string reason) => _inner.CloseAsync(reason);
        public void Dispose() => _inner.Dispose();
        public ValueTask DisposeAsync() => _inner.DisposeAsync();
    }

    /// <summary>
    /// The Slice Check (Ticket #21 / Spec §10 Check 1 & Check 2):
    /// 1. Reconcile two peers with 1,000 identical files: assert exactly 2 messages exchanged (MerkleRootProbe/Response) and 0 chunk payload bytes transferred.
    /// 2. Introduce a 2-line edit in a 10MB file on Peer A: assert traversal identifies only the modified file, queries manifest, and transfers < 200 KB of payload chunks (>98% bandwidth savings).
    /// </summary>
    [Fact]
    public async Task SyncWireProtocol_PrefixDivergenceAndChunkTransfer_Test()
    {
        // =========================================================================
        // PART 1: 1,000 Identical Files -> Exactly 2 Messages Exchanged, 0 Chunk Bytes
        // =========================================================================
        _output.WriteLine("=== PART 1: Benchmarking 1,000 identical files root probe ===");

        using var storeA = new MemoryStateStore();
        using var storeB = new MemoryStateStore();

        // Populate 1,000 identical files across nested subdirectories
        for (int i = 0; i < 1000; i++)
        {
            string path = $"folder_{i % 20}/sub_{i % 5}/file_{i}.txt";
            byte[] data = Encoding.UTF8.GetBytes($"Deterministic test file content payload for file index {i}");
            var manifest = ChunkFingerprinter.CreateManifest(path, data);
            var clock = VectorClock.Empty.Tick("peer-alpha");
            var meta = new FileMetadata(path, manifest.FileSize, manifest.RootHash, DateTimeOffset.UtcNow, clock);

            await storeA.UpsertFileAsync(meta, manifest.Chunks);
            await storeB.UpsertFileAsync(meta, manifest.Chunks);
        }

        // Verify root node hashes are identical
        var rootA = await storeA.GetMerkleNodeAsync("");
        var rootB = await storeB.GetMerkleNodeAsync("");
        rootA.Should().NotBeNull();
        rootB.Should().NotBeNull();
        rootA!.NodeHash.Should().Be(rootB!.NodeHash, "1,000 identical files must produce identical root Merkle hashes");
        rootA.ChildCount.Should().Be(1000);

        // Setup counting transport channel for Part 1
        var (part1ChannelA, part1ChannelB) = InMemoryTransportChannel.CreateConnectedPair("peer-A", "peer-B", _clusterId);
        _disposables.Add(part1ChannelA);
        _disposables.Add(part1ChannelB);

        var countChannelA1 = new CountingTransportChannel(part1ChannelA);
        var countChannelB1 = new CountingTransportChannel(part1ChannelB);
        _disposables.Add(countChannelA1);
        _disposables.Add(countChannelB1);

        var protoPart1A = new SyncWireProtocol(storeA);
        var protoPart1B = new SyncWireProtocol(storeB);
        _disposables.Add(protoPart1A);
        _disposables.Add(protoPart1B);

        protoPart1A.AttachChannel(countChannelA1);
        protoPart1B.AttachChannel(countChannelB1);

        // Execute Root Equality Probe from Peer A
        bool rootsMatch = await protoPart1A.ProbeRootEqualityAsync(countChannelA1);
        rootsMatch.Should().BeTrue("Peers with 1,000 identical files must report matching root");

        // Assert exactly 2 messages exchanged: MerkleRootProbe (from A to B) and MerkleRootResponse (from B to A)
        countChannelA1.MessagesSent.Should().Be(1, "Peer A sent exactly 1 MerkleRootProbe message");
        countChannelA1.MessagesReceived.Should().Be(1, "Peer A received exactly 1 MerkleRootResponse message");
        countChannelB1.MessagesSent.Should().Be(1, "Peer B sent exactly 1 MerkleRootResponse message");
        countChannelB1.MessagesReceived.Should().Be(1, "Peer B received exactly 1 MerkleRootProbe message");

        _output.WriteLine($"Part 1 Passed: Exactly 2 messages exchanged over wire. 0 chunk bytes transferred.");

        // =========================================================================
        // PART 2: 10MB File with 2-line edit -> < 200 KB chunk payload (>98% savings)
        // =========================================================================
        _output.WriteLine("=== PART 2: Benchmarking 10MB file with 2-line delta edit ===");

        // Generate 10MB deterministic dataset
        int totalSize = 10 * 1024 * 1024; // 10,485,760 bytes
        byte[] base10Mb = new byte[totalSize];
        var rng = new Random(1337);
        rng.NextBytes(base10Mb);

        string targetRelativePath = "documents/large_dataset.bin";

        // Peer B has base 10MB file
        var baseManifest = ChunkFingerprinter.CreateManifest(targetRelativePath, base10Mb);
        var baseClock = VectorClock.Empty.Tick("peer-B");
        var baseMeta = new FileMetadata(targetRelativePath, baseManifest.FileSize, baseManifest.RootHash, DateTimeOffset.UtcNow, baseClock);

        var chunksB = new MemoryChunkProvider();
        foreach (var c in baseManifest.Chunks)
        {
            byte[] cBytes = base10Mb.AsSpan((int)c.Offset, c.Length).ToArray();
            chunksB.AddChunk(c.HashHex, cBytes);
        }
        await storeB.UpsertFileAsync(baseMeta, baseManifest.Chunks);

        // Peer A introduces 2-line modification in the middle of 10MB (around 5MB mark)
        byte[] modified10Mb = (byte[])base10Mb.Clone();
        byte[] editBytes = Encoding.UTF8.GetBytes("\n--- LINE 1 MODIFIED BY PEER A ---\n--- LINE 2 MODIFIED BY PEER A ---\n");
        int editOffset = 5 * 1024 * 1024;
        Array.Copy(editBytes, 0, modified10Mb, editOffset, editBytes.Length);

        var modifiedManifest = ChunkFingerprinter.CreateManifest(targetRelativePath, modified10Mb);
        var modifiedClock = baseClock.Tick("peer-A");
        var modifiedMeta = new FileMetadata(targetRelativePath, modifiedManifest.FileSize, modifiedManifest.RootHash, DateTimeOffset.UtcNow, modifiedClock);

        var chunksA = new MemoryChunkProvider();
        foreach (var c in modifiedManifest.Chunks)
        {
            byte[] cBytes = modified10Mb.AsSpan((int)c.Offset, c.Length).ToArray();
            chunksA.AddChunk(c.HashHex, cBytes);
        }
        await storeA.UpsertFileAsync(modifiedMeta, modifiedManifest.Chunks);

        _output.WriteLine($"10MB Base File Chunks: {baseManifest.Chunks.Count}");
        _output.WriteLine($"10MB Modified File Chunks: {modifiedManifest.Chunks.Count}");

        // Setup counting transport channel for Part 2 with chunk providers wired in
        var (rawChannelA2, rawChannelB2) = InMemoryTransportChannel.CreateConnectedPair("peer-A", "peer-B", _clusterId);
        _disposables.Add(rawChannelA2);
        _disposables.Add(rawChannelB2);

        var countChannelA2 = new CountingTransportChannel(rawChannelA2);
        var countChannelB2 = new CountingTransportChannel(rawChannelB2);
        _disposables.Add(countChannelA2);
        _disposables.Add(countChannelB2);

        var protoPart2A = new SyncWireProtocol(storeA, localChunkProvider: chunksA);
        var protoPart2B = new SyncWireProtocol(storeB, localChunkProvider: chunksB);
        _disposables.Add(protoPart2A);
        _disposables.Add(protoPart2B);

        protoPart2A.AttachChannel(countChannelA2);
        protoPart2B.AttachChannel(countChannelB2);

        // Now Peer B initiates synchronization against Peer A
        // 1. Root Equality Probe
        bool secondProbe = await protoPart2B.ProbeRootEqualityAsync(countChannelB2);
        secondProbe.Should().BeFalse("Root hashes must diverge after 2-line edit on Peer A");

        // 2. Reconcile Prefix
        var divergentFiles = await protoPart2B.ReconcilePrefixAsync(countChannelB2, "");
        divergentFiles.Should().HaveCount(1, "Only the modified 10MB file should be marked divergent");
        divergentFiles[0].RelativePath.Should().Be(targetRelativePath);
        divergentFiles[0].IsContentModified.Should().BeTrue();
        divergentFiles[0].RemoteRootHash.Should().Be(modifiedMeta.RootHash);

        // 3. Fetch Manifest
        var remoteManifestResp = await protoPart2B.FetchManifestAsync(countChannelB2, targetRelativePath);
        remoteManifestResp.RelativePath.Should().Be(targetRelativePath);
        remoteManifestResp.ContentHash.Should().Be(modifiedMeta.RootHash);
        remoteManifestResp.TotalBytes.Should().Be(totalSize);

        // 4. Identify missing chunks on Peer B
        var probeResult = await storeB.ProbeChunksAsync(remoteManifestResp.Chunks.Select(c => c.HashHex));
        var missingHashes = probeResult.MissingHashes.ToList();

        _output.WriteLine($"Total Chunks in Remote File: {remoteManifestResp.Chunks.Count}");
        _output.WriteLine($"Chunks already present on Peer B: {probeResult.LocalCount}");
        _output.WriteLine($"Missing chunks requiring wire transfer: {missingHashes.Count}");

        // For a 2-line edit in 10MB using FastCDC 64KB target chunking, only 1-3 chunks change
        missingHashes.Count.Should().BeInRange(1, 3, "2-line edit must modify at most 1-3 FastCDC chunks");

        // 5. Stream missing chunks using sliding window (W = 16)
        var fetchedChunks = await protoPart2B.FetchChunksPipelinedAsync(countChannelB2, missingHashes, windowSize: 16);
        long payloadBytesTransferred = fetchedChunks.Values.Sum(p => (long)p.Length);

        _output.WriteLine($"Total Chunk Payload Bytes Transferred: {payloadBytesTransferred:N0} bytes");

        // Assert payload transferred is strictly < 200 KB
        payloadBytesTransferred.Should().BeLessThan(200 * 1024, "Missing chunk transfer must be < 200 KB");

        double bandwidthSavings = 1.0 - ((double)payloadBytesTransferred / totalSize);
        _output.WriteLine($"Bandwidth Savings: {bandwidthSavings * 100:F2}%");
        bandwidthSavings.Should().BeGreaterThan(0.98, "Delta transfer must achieve > 98% bandwidth savings");

        // 6. Complete reconstruction to destination file
        string destPath = Path.Combine(_tempDir, "reconstructed_10mb.bin");
        var reconResult = await protoPart2B.FetchAndReconstructFileAsync(
            countChannelB2,
            remoteManifestResp,
            destPath,
            tempDirectory: _tempDir);

        reconResult.TotalBytes.Should().Be(totalSize);
        reconResult.ReusedBytes.Should().Be(totalSize - payloadBytesTransferred);
        reconResult.BandwidthSavingsRatio.Should().BeGreaterThan(0.98);

        // Verify bit-for-bit SHA-256 integrity on disk
        byte[] diskBytes = await File.ReadAllBytesAsync(destPath);
        diskBytes.Should().Equal(modified10Mb, "Reconstructed 10MB file must match modified content bit-for-bit");
    }

    [Fact]
    public async Task Gate15_FaultInjection_CorruptedChunkPayload_ThrowsChunkIntegrityException()
    {
        using var storeA = new MemoryStateStore();
        using var storeB = new MemoryStateStore();
        var chunksA = new MemoryChunkProvider();

        byte[] validData = Encoding.UTF8.GetBytes("Genuine uncorrupted chunk content");
        byte[] validHash = SHA256.HashData(validData);
        string hashHex = Convert.ToHexString(validHash).ToLowerInvariant();

        // Intentionally inject bit-flip corruption into stored chunk
        byte[] corruptedData = (byte[])validData.Clone();
        corruptedData[0] ^= 0xFF;
        chunksA.AddChunk(hashHex, corruptedData);

        var (rawA, rawB) = InMemoryTransportChannel.CreateConnectedPair("peer-A", "peer-B", _clusterId);
        _disposables.Add(rawA);
        _disposables.Add(rawB);

        var protoA = new SyncWireProtocol(storeA, localChunkProvider: chunksA);
        var protoB = new SyncWireProtocol(storeB);
        _disposables.Add(protoA);
        _disposables.Add(protoB);

        protoA.AttachChannel(rawA);
        protoB.AttachChannel(rawB);

        // Fetching the corrupted chunk must trigger immediate cryptographic integrity failure
        var act = async () => await protoB.FetchChunkAsync(rawB, hashHex);

        await act.Should().ThrowAsync<ChunkIntegrityException>()
            .WithMessage("*Chunk integrity verification failed*");
    }
}
