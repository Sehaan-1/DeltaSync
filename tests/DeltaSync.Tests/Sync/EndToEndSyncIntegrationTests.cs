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

namespace DeltaSync.Tests.Sync;

public class EndToEndSyncIntegrationTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly Guid _clusterId = Guid.NewGuid();
    private readonly string _testBaseDir;
    private readonly List<IAsyncDisposable> _disposables = new();

    public EndToEndSyncIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
        _testBaseDir = Path.Combine(Path.GetTempPath(), $"deltasync_e2e_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_testBaseDir);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var d in _disposables)
        {
            try { await d.DisposeAsync(); } catch { }
        }

        DatabaseCrashRecovery.ClearPools();

        if (Directory.Exists(_testBaseDir))
        {
            try { Directory.Delete(_testBaseDir, recursive: true); } catch { }
        }
    }

    private async Task<(
        string RootDir,
        SqliteStateStore Store,
        SqliteLocalChunkProvider ChunkProvider,
        LocalFileIngestor Ingestor,
        FileWatcherService Watcher,
        SyncWireProtocol Wire,
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
        _disposables.Add(store);

        var chunkProvider = new SqliteLocalChunkProvider(store, nodeDir);
        var ingestor = new LocalFileIngestor(nodeDir, store, peerId);

        var watcher = new FileWatcherService(nodeDir, store, peerId, debounceWindow: TimeSpan.FromMilliseconds(50));
        _disposables.Add(watcher);

        var wire = new SyncWireProtocol(store, chunkProvider, nodeDir);
        _disposables.Add(wire);

        var orchestrator = new SyncOrchestrator(
            syncRootDirectory: nodeDir,
            stateStore: store,
            wireProtocol: wire,
            watcherService: watcher,
            ingestor: ingestor,
            localPeerId: peerId);
        _disposables.Add(orchestrator);

        return (nodeDir, store, chunkProvider, ingestor, watcher, wire, orchestrator);
    }

    /// <summary>
    /// The Slice 3 Check (Ticket #22 / Handoff §3):
    /// 1. Spin up two mock peer instances with local SQLite state stores and temporary folders connected via InMemoryTransportChannel.
    /// 2. Machine A creates 10 files; assert Machine B converges bit-for-bit to identical directory structure.
    /// 3. Disconnect channel. Machine A edits doc.txt; Machine B simultaneously edits doc.txt.
    /// 4. Reconnect channel. Run sync: assert doc.txt and conflicted sibling both exist on both machines with zero data loss,
    ///    matching content hashes, and identical final Merkle root digests.
    /// </summary>
    [Fact]
    public async Task EndToEndSync_TwoPeers_DeltasAndConflicts_Converge_Test()
    {
        // 1. Arrange: Spin up Node A and Node B
        var nodeA = await CreateNodeAsync("MachineA");
        var nodeB = await CreateNodeAsync("MachineB");

        await nodeA.Orchestrator.StartAsync();
        await nodeB.Orchestrator.StartAsync();

        // 2. Machine A creates 10 files (including nested subdirectories and doc.txt)
        var createdFiles = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (int i = 1; i <= 8; i++)
        {
            string relPath = $"file_{i}.txt";
            string content = $"Payload content for initial file {i} on Machine A.\nLine 2.\nLine 3.";
            createdFiles[relPath] = content;
            await File.WriteAllTextAsync(Path.Combine(nodeA.RootDir, relPath), content);
        }

        // Add a nested file
        string nestedRel = "subfolder/nested_note.txt";
        string nestedContent = "Nested directory structure synchronization test.";
        createdFiles[nestedRel] = nestedContent;
        Directory.CreateDirectory(Path.Combine(nodeA.RootDir, "subfolder"));
        await File.WriteAllTextAsync(Path.Combine(nodeA.RootDir, nestedRel), nestedContent);

        // Add the primary document that will later experience concurrent conflict
        string docRel = "doc.txt";
        string docInitialContent = "Original base content of doc.txt shared before any offline partitioning.";
        createdFiles[docRel] = docInitialContent;
        await File.WriteAllTextAsync(Path.Combine(nodeA.RootDir, docRel), docInitialContent);

        createdFiles.Count.Should().Be(10, "Machine A must create exactly 10 initial files.");

        // Ingest all files on Node A
        int ingested = await nodeA.Ingestor.IngestAllAsync();
        ingested.Should().Be(10);

        var initialRootA = await nodeA.Store.GetMerkleNodeAsync("");
        initialRootA.Should().NotBeNull();
        initialRootA!.NodeHash.Should().NotBe(MerkleTreeHelper.EmptyNodeHash);

        // Connect Machine A and Machine B via InMemoryTransportChannel
        var (chanA, chanB) = InMemoryTransportChannel.CreateConnectedPair("MachineA", "MachineB", _clusterId);
        _disposables.Add(chanA);
        _disposables.Add(chanB);

        nodeA.Orchestrator.AttachChannel(chanA);
        nodeB.Orchestrator.AttachChannel(chanB);

        // Machine B triggers sync to converge with Machine A (or waits for auto-sync)
        await nodeB.Orchestrator.SynchronizeAsync(chanB);

        // Assert Machine B converges bit-for-bit to identical directory structure
        foreach (var (relPath, expectedText) in createdFiles)
        {
            string fullPathB = Path.Combine(nodeB.RootDir, relPath);
            File.Exists(fullPathB).Should().BeTrue($"File '{relPath}' must exist on Machine B.");
            string actualTextB = await File.ReadAllTextAsync(fullPathB);
            actualTextB.Should().Be(expectedText, $"Content of '{relPath}' on Machine B must match Machine A exactly.");
        }

        var rootBAfterInitial = await nodeB.Store.GetMerkleNodeAsync("");
        rootBAfterInitial.Should().NotBeNull();
        rootBAfterInitial!.NodeHash.Should().Be(initialRootA.NodeHash, "Merkle roots must match identically after initial sync.");

        _output.WriteLine($"[Phase 1 Passed] 10 files synchronized to Machine B. Identical Merkle root: {initialRootA.NodeHash[..12]}...");

        // 3. Disconnect transport channels to simulate network partition / offline work
        await chanA.CloseAsync("Simulate offline disconnect");
        await chanB.CloseAsync("Simulate offline disconnect");

        // Machine A edits doc.txt offline
        string contentA = "Machine A made offline modifications to doc.txt with important updates.";
        await File.WriteAllTextAsync(Path.Combine(nodeA.RootDir, "doc.txt"), contentA);
        await nodeA.Ingestor.IngestFileAsync("doc.txt");

        // Machine B simultaneously edits doc.txt offline
        string contentB = "Machine B concurrently made different offline changes to doc.txt!";
        await File.WriteAllTextAsync(Path.Combine(nodeB.RootDir, "doc.txt"), contentB);
        await nodeB.Ingestor.IngestFileAsync("doc.txt");

        // Verify divergence during offline partition
        var offlineMetaA = await nodeA.Store.GetFileAsync("doc.txt");
        var offlineMetaB = await nodeB.Store.GetFileAsync("doc.txt");
        offlineMetaA!.RootHash.Should().NotBe(offlineMetaB!.RootHash);
        offlineMetaA.Clock.Compare(offlineMetaB.Clock).Should().Be(CausalRelation.Concurrent);

        var offlineRootA = await nodeA.Store.GetMerkleNodeAsync("");
        var offlineRootB = await nodeB.Store.GetMerkleNodeAsync("");
        offlineRootA!.NodeHash.Should().NotBe(offlineRootB!.NodeHash, "Merkle roots must diverge during partition.");

        _output.WriteLine("[Phase 2 Passed] Machine A and B introduced concurrent edits. Clocks are concurrent.");

        // 4. Reconnect channel and run bidirectional sync
        var (reconnA, reconnB) = InMemoryTransportChannel.CreateConnectedPair("MachineA", "MachineB", _clusterId);
        _disposables.Add(reconnA);
        _disposables.Add(reconnB);

        nodeA.Orchestrator.AttachChannel(reconnA);
        nodeB.Orchestrator.AttachChannel(reconnB);

        // Bidirectional sync convergence
        await nodeA.Orchestrator.SynchronizeAsync(reconnA);
        await nodeB.Orchestrator.SynchronizeAsync(reconnB);

        // 5. Verification: Both versions preserved side-by-side (ADR-0001) with zero data loss
        string primaryDocA = Path.Combine(nodeA.RootDir, "doc.txt");
        string primaryDocB = Path.Combine(nodeB.RootDir, "doc.txt");

        File.Exists(primaryDocA).Should().BeTrue();
        File.Exists(primaryDocB).Should().BeTrue();

        // Find the conflict sibling file on both machines (matching *conflict*)
        var dirInfoA = new DirectoryInfo(nodeA.RootDir);
        var dirInfoB = new DirectoryInfo(nodeB.RootDir);

        var conflictFilesA = dirInfoA.EnumerateFiles("*conflict*", SearchOption.TopDirectoryOnly).ToList();
        var conflictFilesB = dirInfoB.EnumerateFiles("*conflict*", SearchOption.TopDirectoryOnly).ToList();

        conflictFilesA.Should().HaveCount(1, "Machine A must contain exactly one conflict sibling file.");
        conflictFilesB.Should().HaveCount(1, "Machine B must contain exactly one conflict sibling file.");

        var conflictFileA = conflictFilesA[0];
        var conflictFileB = conflictFilesB[0];

        conflictFileA.Name.Should().Be(conflictFileB.Name, "Conflict file names must match across machines.");

        string conflictContentA = await File.ReadAllTextAsync(conflictFileA.FullName);
        string conflictContentB = await File.ReadAllTextAsync(conflictFileB.FullName);
        conflictContentA.Should().Be(conflictContentB, "Conflict file contents must be identical across machines.");

        string primaryContentA = await File.ReadAllTextAsync(primaryDocA);
        string primaryContentB = await File.ReadAllTextAsync(primaryDocB);
        primaryContentA.Should().Be(primaryContentB, "Primary doc.txt contents must be identical across machines.");

        // Zero data loss invariant: both contentA and contentB must be preserved
        var allContents = new HashSet<string> { primaryContentA, conflictContentA };
        allContents.Should().Contain(contentA, "Machine A's offline edit must be fully preserved.");
        allContents.Should().Contain(contentB, "Machine B's offline edit must be fully preserved.");

        // Cryptographic whole-directory convergence: identical Merkle roots
        var finalRootA = await nodeA.Store.GetMerkleNodeAsync("");
        var finalRootB = await nodeB.Store.GetMerkleNodeAsync("");

        finalRootA.Should().NotBeNull();
        finalRootB.Should().NotBeNull();
        finalRootA!.NodeHash.Should().Be(finalRootB!.NodeHash, "Both peers must converge to identical Merkle root hash.");
        finalRootA.NodeHash.Should().NotBe(MerkleTreeHelper.EmptyNodeHash);

        _output.WriteLine($"[Phase 3 Passed] Convergence complete with zero data loss. Final Merkle root: {finalRootA.NodeHash[..12]}...");
        _output.WriteLine($"Preserved files: '{primaryDocA}' and '{conflictFileA.FullName}'.");
    }
}
