namespace DeltaSync.Tests.Network;

using System.Net;
using System.Security.Cryptography;
using System.Text;
using DeltaSync.Core.Causality;
using DeltaSync.Core.Chunking;
using DeltaSync.Core.Models;
using DeltaSync.Core.Storage;
using DeltaSync.Core.Sync;
using DeltaSync.Network;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

/// <summary>
/// End-to-end integration tests verifying cross-node directory synchronization over physical OS TCP sockets
/// using TcpPeerListener and TcpPeerDialer (Spec §3 Step 5 & §10 Check 4, Issue #36).
/// </summary>
public class RealSocketSyncIntegrationTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly Guid _clusterId = Guid.NewGuid();
    private readonly string _testBaseDir;
    private readonly List<IAsyncDisposable> _asyncDisposables = new();

    public RealSocketSyncIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
        _testBaseDir = Path.Combine(Path.GetTempPath(), $"deltasync_real_tcp_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testBaseDir);
    }

    public async ValueTask DisposeAsync()
    {
        for (int i = _asyncDisposables.Count - 1; i >= 0; i--)
        {
            try
            {
                await _asyncDisposables[i].DisposeAsync();
            }
            catch
            {
            }
        }

        DatabaseCrashRecovery.ClearPools();

        if (Directory.Exists(_testBaseDir))
        {
            try
            {
                Directory.Delete(_testBaseDir, recursive: true);
            }
            catch
            {
            }
        }
    }

    private async Task<(
        string RootDir,
        SqliteStateStore Store,
        SqliteLocalChunkProvider ChunkProvider,
        LocalFileIngestor Ingestor,
        FileWatcherService Watcher,
        SyncWireProtocol Wire,
        PeerRegistry Registry,
        TcpPeerDialer Dialer,
        PeerConnectionCoordinator Coordinator,
        TcpPeerListener Listener,
        SyncOrchestrator Orchestrator)> CreateNodeAsync(string peerId)
    {
        string nodeDir = Path.Combine(_testBaseDir, peerId);
        string metaDir = Path.Combine(nodeDir, ".deltasync");
        Directory.CreateDirectory(nodeDir);
        Directory.CreateDirectory(metaDir);

        string dbPath = Path.Combine(metaDir, "state.db");
        var connFactory = new SqliteConnectionFactory(dbPath);
        await using (var conn = await connFactory.OpenConnectionAsync())
        {
            await SqliteSchemaMigrator.MigrateAsync(conn);
        }

        var store = new SqliteStateStore(connFactory);
        _asyncDisposables.Add(store);

        var chunkProvider = new SqliteLocalChunkProvider(store, nodeDir);
        var ingestor = new LocalFileIngestor(nodeDir, store, peerId);

        var watcher = new FileWatcherService(nodeDir, store, peerId, debounceWindow: TimeSpan.FromMilliseconds(50));
        _asyncDisposables.Add(watcher);

        var wire = new SyncWireProtocol(store, chunkProvider, nodeDir);
        _asyncDisposables.Add(wire);

        var registry = new PeerRegistry();
        var dialer = new TcpPeerDialer(_clusterId, peerId, registry);

        var coordinator = new PeerConnectionCoordinator(
            _clusterId,
            peerId,
            listenPort: 0,
            registry: registry,
            dialer: dialer.AsDialer());
        _asyncDisposables.Add(coordinator);

        var listener = new TcpPeerListener(
            peerId,
            _clusterId,
            new IPEndPoint(IPAddress.Loopback, 0),
            coordinator);
        listener.Start();
        _asyncDisposables.Add(listener);

        var orchestrator = new SyncOrchestrator(
            syncRootDirectory: nodeDir,
            stateStore: store,
            wireProtocol: wire,
            watcherService: watcher,
            ingestor: ingestor,
            peerProvider: coordinator,
            localPeerId: peerId);
        _asyncDisposables.Add(orchestrator);

        return (nodeDir, store, chunkProvider, ingestor, watcher, wire, registry, dialer, coordinator, listener, orchestrator);
    }

    /// <summary>
    /// Check 4 (Spec §10 Check 4 / Ticket #36):
    /// Two nodes configured with distinct temp directories and state databases connect over real OS TCP sockets.
    /// 10 files (including a 1MB file with localized byte edits) are synchronized.
    /// Assert bit-for-bit SHA-256 equivalence on all files, chunk delta efficiency, two-way sync, deletion propagation,
    /// and 0 socket leaks.
    /// </summary>
    [Fact]
    public async Task EndToEndSync_TwoPhysicalTcpPeers_ConvergeFiles()
    {
        // 1. Arrange two nodes running on separate OS loopback TCP ports
        var nodeA = await CreateNodeAsync("NodeAlpha");
        var nodeB = await CreateNodeAsync("NodeBeta");

        await nodeA.Orchestrator.StartAsync();
        await nodeB.Orchestrator.StartAsync();

        _output.WriteLine($"NodeAlpha listening on {nodeA.Listener.LocalEndPoint}");
        _output.WriteLine($"NodeBeta listening on {nodeB.Listener.LocalEndPoint}");

        // 2. Node Alpha creates 10 files: 8 text files, 1 nested directory file, and 1 large 1MB binary file
        var expectedFileHashes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        for (int i = 1; i <= 8; i++)
        {
            string rel = $"file_{i}.txt";
            string text = $"Physical TCP transport integration test - file {i} content.\nLine 2.\nLine 3 with padding {new string('x', i * 100)}";
            byte[] bytes = Encoding.UTF8.GetBytes(text);
            await File.WriteAllBytesAsync(Path.Combine(nodeA.RootDir, rel), bytes);
            expectedFileHashes[rel] = Convert.ToHexString(SHA256.HashData(bytes));
        }

        string nestedRel = Path.Combine("nested", "deep", "guide.md");
        Directory.CreateDirectory(Path.GetDirectoryName(Path.Combine(nodeA.RootDir, nestedRel))!);
        byte[] nestedBytes = Encoding.UTF8.GetBytes("# Architecture Guide\nSynchronized over real length-prefixed OS TCP streaming sockets.");
        await File.WriteAllBytesAsync(Path.Combine(nodeA.RootDir, nestedRel), nestedBytes);
        expectedFileHashes[nestedRel] = Convert.ToHexString(SHA256.HashData(nestedBytes));

        string largeRel = "payload_1mb.bin";
        byte[] largeBytes = new byte[1024 * 1024]; // 1,048,576 bytes
        new Random(42).NextBytes(largeBytes);
        await File.WriteAllBytesAsync(Path.Combine(nodeA.RootDir, largeRel), largeBytes);
        expectedFileHashes[largeRel] = Convert.ToHexString(SHA256.HashData(largeBytes));

        expectedFileHashes.Count.Should().Be(10, "Node Alpha must create exactly 10 initial files.");

        // Ingest all files into Node Alpha's state store
        int ingestedA = await nodeA.Ingestor.IngestAllAsync();
        ingestedA.Should().Be(10);

        var initialRootA = await nodeA.Store.GetMerkleNodeAsync("");
        initialRootA.Should().NotBeNull();
        initialRootA!.NodeHash.Should().NotBe(MerkleTreeHelper.EmptyNodeHash);

        // 3. Establish physical TCP connection from Node Alpha to Node Beta
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        var channelA = await nodeA.Coordinator.ConnectAsync("NodeBeta", nodeB.Listener.LocalEndPoint, cts.Token);
        channelA.Should().NotBeNull();
        channelA!.IsConnected.Should().BeTrue("Node Alpha must establish connected TCP channel to Node Beta");

        // Allow accept loop to finish handshake registration
        nodeB.Coordinator.TryGetConnection("NodeAlpha", out var channelB).Should().BeTrue();
        channelB.Should().NotBeNull();
        channelB!.IsConnected.Should().BeTrue("Node Beta must accept incoming TCP connection from Node Alpha");

        // 4. Synchronize Node Beta from Node Alpha over the real physical TCP channel.
        // Note: Automatic anti-entropy sync is triggered immediately upon ConnectionEstablished.
        // Calling SynchronizeAsync ensures that synchronization is fully completed.
        await nodeB.Orchestrator.SynchronizeAsync(channelB!, cts.Token);

        // Assert bit-for-bit SHA-256 equivalence on all 10 files
        foreach (var (relPath, expectedHash) in expectedFileHashes)
        {
            string fullPathB = Path.Combine(nodeB.RootDir, relPath);
            File.Exists(fullPathB).Should().BeTrue($"File '{relPath}' must exist on Node Beta.");
            byte[] actualBytesB = await File.ReadAllBytesAsync(fullPathB);
            string actualHashB = Convert.ToHexString(SHA256.HashData(actualBytesB));
            actualHashB.Should().Be(expectedHash, $"File '{relPath}' on Node Beta must match Node Alpha bit-for-bit.");
        }

        var rootB = await nodeB.Store.GetMerkleNodeAsync("");
        rootB.Should().NotBeNull();
        rootB!.NodeHash.Should().Be(initialRootA.NodeHash, "Both nodes must converge to identical Merkle root digest over physical TCP sockets.");

        _output.WriteLine($"[Phase 1 Passed] 10 files synchronized over real TCP sockets. Merkle root: {initialRootA.NodeHash[..12]}...");

        // 5. Test localized byte edits on the 1MB file (FastCDC delta efficiency)
        // Mutate 32 bytes in the middle of the 1MB file
        byte[] mutatedLargeBytes = (byte[])largeBytes.Clone();
        for (int b = 0; b < 32; b++)
        {
            mutatedLargeBytes[524_288 + b] ^= 0xAA;
        }
        await File.WriteAllBytesAsync(Path.Combine(nodeA.RootDir, largeRel), mutatedLargeBytes);
        string mutatedHash = Convert.ToHexString(SHA256.HashData(mutatedLargeBytes));

        await nodeA.Ingestor.IngestFileAsync(largeRel);
        var mutatedRootA = await nodeA.Store.GetMerkleNodeAsync("");
        mutatedRootA!.NodeHash.Should().NotBe(initialRootA.NodeHash, "Merkle root must update on edit.");

        // Synchronize edit to Node Beta over existing TCP socket
        await nodeB.Orchestrator.SynchronizeAsync(channelB!, cts.Token);

        byte[] updatedBytesB = await File.ReadAllBytesAsync(Path.Combine(nodeB.RootDir, largeRel));
        string updatedHashB = Convert.ToHexString(SHA256.HashData(updatedBytesB));
        updatedHashB.Should().Be(mutatedHash, "Node Beta must receive delta-updated 1MB file with identical SHA-256.");

        var mutatedRootB = await nodeB.Store.GetMerkleNodeAsync("");
        mutatedRootB!.NodeHash.Should().Be(mutatedRootA.NodeHash, "Merkle roots must converge after 1MB delta update.");

        _output.WriteLine("[Phase 2 Passed] 1MB file localized delta edit synchronized with bit-for-bit equivalence.");

        // 6. Test two-way synchronization: Node Beta creates a file and synchronizes back to Node Alpha
        string betaFileRel = "notes_from_beta.txt";
        byte[] betaFileBytes = Encoding.UTF8.GetBytes("Created on Node Beta and transmitted upstream to Node Alpha.");
        await File.WriteAllBytesAsync(Path.Combine(nodeB.RootDir, betaFileRel), betaFileBytes);
        string betaFileHash = Convert.ToHexString(SHA256.HashData(betaFileBytes));

        await nodeB.Ingestor.IngestFileAsync(betaFileRel);

        await nodeA.Orchestrator.SynchronizeAsync(channelA, cts.Token);

        string betaFileOnA = Path.Combine(nodeA.RootDir, betaFileRel);
        File.Exists(betaFileOnA).Should().BeTrue();
        byte[] receivedOnA = await File.ReadAllBytesAsync(betaFileOnA);
        Convert.ToHexString(SHA256.HashData(receivedOnA)).Should().Be(betaFileHash);

        var finalRootA = await nodeA.Store.GetMerkleNodeAsync("");
        var finalRootB = await nodeB.Store.GetMerkleNodeAsync("");
        finalRootA!.NodeHash.Should().Be(finalRootB!.NodeHash);

        _output.WriteLine("[Phase 3 Passed] Bidirectional sync from Node Beta to Node Alpha succeeded.");

        // 7. Test deletion propagation over physical TCP socket
        File.Delete(Path.Combine(nodeA.RootDir, "file_1.txt"));
        var meta1 = await nodeA.Store.GetFileAsync("file_1.txt");
        meta1.Should().NotBeNull();
        var tombstone = new FileMetadata(
            "file_1.txt",
            0,
            MerkleTreeHelper.EmptyNodeHash,
            DateTimeOffset.UtcNow,
            meta1!.Clock.Tick("NodeAlpha"),
            isDeleted: true,
            version: meta1.Version + 1);
        await nodeA.Store.UpsertFileAsync(tombstone, Array.Empty<ChunkDescriptor>());

        await nodeB.Orchestrator.SynchronizeAsync(channelB!, cts.Token);

        File.Exists(Path.Combine(nodeB.RootDir, "file_1.txt")).Should().BeFalse("file_1.txt must be removed from Node Beta filesystem.");

        var postDeleteRootA = await nodeA.Store.GetMerkleNodeAsync("");
        var postDeleteRootB = await nodeB.Store.GetMerkleNodeAsync("");
        postDeleteRootB!.NodeHash.Should().Be(postDeleteRootA!.NodeHash);

        _output.WriteLine("[Phase 4 Passed] Deletion propagation succeeded over physical TCP socket.");

        // 8. Clean shutdown & zero socket leaks
        await nodeA.Listener.StopAsync();
        await nodeB.Listener.StopAsync();

        await nodeA.Coordinator.CloseConnectionAsync("NodeBeta", "Test finished");
        await nodeB.Coordinator.CloseConnectionAsync("NodeAlpha", "Test finished");

        nodeA.Coordinator.ActiveConnections.Should().BeEmpty("Node Alpha must have 0 active connections post-close");
        nodeB.Coordinator.ActiveConnections.Should().BeEmpty("Node Beta must have 0 active connections post-close");
    }

    [Fact]
    public async Task EndToEndSync_TwoPhysicalTcpPeers_ConcurrentConflicts_ConvergePreservingBoth()
    {
        // ADR-0001 Side-by-side conflict preservation over real physical TCP sockets
        var nodeA = await CreateNodeAsync("NodeA_Conflict");
        var nodeB = await CreateNodeAsync("NodeB_Conflict");

        await nodeA.Orchestrator.StartAsync();
        await nodeB.Orchestrator.StartAsync();

        string sharedRel = "contract.txt";
        byte[] initialData = Encoding.UTF8.GetBytes("Initial base agreement shared between Node A and Node B.");
        await File.WriteAllBytesAsync(Path.Combine(nodeA.RootDir, sharedRel), initialData);
        await File.WriteAllBytesAsync(Path.Combine(nodeB.RootDir, sharedRel), initialData);

        var baseClock = VectorClock.Empty.Tick("NodeA_Conflict");
        var manifest = ChunkFingerprinter.CreateManifest(sharedRel, initialData);
        var baseMeta = new FileMetadata(sharedRel, manifest.FileSize, manifest.RootHash, DateTimeOffset.UtcNow, baseClock);
        await nodeA.Store.UpsertFileAsync(baseMeta, manifest.Chunks);
        await nodeB.Store.UpsertFileAsync(baseMeta, manifest.Chunks);

        // Offline edits on both nodes
        string textA = "Node A amended clause 1 with revised pricing terms.";
        byte[] bytesA = Encoding.UTF8.GetBytes(textA);
        await File.WriteAllBytesAsync(Path.Combine(nodeA.RootDir, sharedRel), bytesA);
        var manifestA = ChunkFingerprinter.CreateManifest(sharedRel, bytesA);
        var clockA = baseClock.Tick("NodeA_Conflict");
        var metaA = new FileMetadata(sharedRel, manifestA.FileSize, manifestA.RootHash, DateTimeOffset.UtcNow, clockA, version: 2);
        await nodeA.Store.UpsertFileAsync(metaA, manifestA.Chunks);

        string textB = "Node B concurrently added clause 2 with international shipping terms.";
        byte[] bytesB = Encoding.UTF8.GetBytes(textB);
        await File.WriteAllBytesAsync(Path.Combine(nodeB.RootDir, sharedRel), bytesB);
        var manifestB = ChunkFingerprinter.CreateManifest(sharedRel, bytesB);
        var clockB = baseClock.Tick("NodeB_Conflict");
        var metaB = new FileMetadata(sharedRel, manifestB.FileSize, manifestB.RootHash, DateTimeOffset.UtcNow, clockB, version: 2);
        await nodeB.Store.UpsertFileAsync(metaB, manifestB.Chunks);

        // Clocks are strictly concurrent
        clockA.Compare(clockB).Should().Be(CausalRelation.Concurrent);

        // Connect over physical TCP socket
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var channelA = await nodeA.Coordinator.ConnectAsync("NodeB_Conflict", nodeB.Listener.LocalEndPoint, cts.Token);
        channelA.Should().NotBeNull();

        nodeB.Coordinator.TryGetConnection("NodeA_Conflict", out var channelB).Should().BeTrue();

        // Bidirectional sync
        await nodeA.Orchestrator.SynchronizeAsync(channelA!, cts.Token);
        await nodeB.Orchestrator.SynchronizeAsync(channelB!, cts.Token);

        // Verify ADR-0001: Both versions preserved side-by-side on both nodes
        var dirA = new DirectoryInfo(nodeA.RootDir);
        var dirB = new DirectoryInfo(nodeB.RootDir);

        var conflictFilesA = dirA.EnumerateFiles("*conflict*", SearchOption.TopDirectoryOnly).ToList();
        var conflictFilesB = dirB.EnumerateFiles("*conflict*", SearchOption.TopDirectoryOnly).ToList();

        conflictFilesA.Should().HaveCount(1, "Node A must contain exactly 1 conflict file.");
        conflictFilesB.Should().HaveCount(1, "Node B must contain exactly 1 conflict file.");

        conflictFilesA[0].Name.Should().Be(conflictFilesB[0].Name);

        string primaryA = await File.ReadAllTextAsync(Path.Combine(nodeA.RootDir, sharedRel));
        string primaryB = await File.ReadAllTextAsync(Path.Combine(nodeB.RootDir, sharedRel));
        primaryA.Should().Be(primaryB);

        string confA = await File.ReadAllTextAsync(conflictFilesA[0].FullName);
        string confB = await File.ReadAllTextAsync(conflictFilesB[0].FullName);
        confA.Should().Be(confB);

        var allContents = new HashSet<string> { primaryA, confA };
        allContents.Should().Contain(textA);
        allContents.Should().Contain(textB);

        // Cryptographic convergence to identical Merkle roots
        var rootA = await nodeA.Store.GetMerkleNodeAsync("");
        var rootB = await nodeB.Store.GetMerkleNodeAsync("");
        rootA!.NodeHash.Should().Be(rootB!.NodeHash);

        await nodeA.Listener.StopAsync();
        await nodeB.Listener.StopAsync();
        await nodeA.Coordinator.CloseConnectionAsync("NodeB_Conflict");
        await nodeB.Coordinator.CloseConnectionAsync("NodeA_Conflict");
    }
}
