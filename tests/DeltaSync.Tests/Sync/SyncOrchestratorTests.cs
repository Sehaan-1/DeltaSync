using System.Text;
using DeltaSync.Core.Causality;
using DeltaSync.Core.Chunking;
using DeltaSync.Core.Models;
using DeltaSync.Core.Storage;
using DeltaSync.Core.Sync;
using DeltaSync.Network;
using FluentAssertions;
using Xunit;

namespace DeltaSync.Tests.Sync;

public class SyncOrchestratorTests : IAsyncDisposable
{
    private readonly Guid _clusterId = Guid.NewGuid();
    private readonly string _tempDir;
    private readonly List<IAsyncDisposable> _disposables = new();

    public SyncOrchestratorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"deltasync_orchestrator_test_{Guid.NewGuid():N}");
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

    [Fact]
    public void CleanupStagingFiles_RemovesOrphanedTmpFiles_Test()
    {
        string staging = Path.Combine(_tempDir, ".deltasync", "staging");
        string tmp = Path.Combine(_tempDir, ".deltasync", "tmp");
        Directory.CreateDirectory(staging);
        Directory.CreateDirectory(tmp);

        File.WriteAllText(Path.Combine(staging, "orphaned1.tmp"), "test1");
        File.WriteAllText(Path.Combine(staging, "orphaned2.tmp"), "test2");
        File.WriteAllText(Path.Combine(tmp, "orphaned3.tmp"), "test3");
        File.WriteAllText(Path.Combine(staging, "keep_me.txt"), "keep");

        using var store = new MemoryStateStore();
        using var wire = new SyncWireProtocol(store);
        using var orchestrator = new SyncOrchestrator(_tempDir, store, wire);

        int cleaned = orchestrator.CleanupStagingFiles();

        cleaned.Should().Be(3);
        File.Exists(Path.Combine(staging, "orphaned1.tmp")).Should().BeFalse();
        File.Exists(Path.Combine(staging, "orphaned2.tmp")).Should().BeFalse();
        File.Exists(Path.Combine(tmp, "orphaned3.tmp")).Should().BeFalse();
        File.Exists(Path.Combine(staging, "keep_me.txt")).Should().BeTrue();
    }

    [Fact]
    public async Task StartAndStop_ManagesLifecycleAndWatcher_Test()
    {
        using var store = new MemoryStateStore();
        using var wire = new SyncWireProtocol(store);
        using var orchestrator = new SyncOrchestrator(_tempDir, store, wire);

        orchestrator.IsRunning.Should().BeFalse();

        await orchestrator.StartAsync();
        orchestrator.IsRunning.Should().BeTrue();

        await orchestrator.StopAsync();
        orchestrator.IsRunning.Should().BeFalse();
    }

    [Fact]
    public async Task RemoteDeletion_AppliesTombstoneAndDeletesFile_Test()
    {
        string dirA = Path.Combine(_tempDir, "NodeA");
        string dirB = Path.Combine(_tempDir, "NodeB");
        Directory.CreateDirectory(dirA);
        Directory.CreateDirectory(dirB);

        var (chanA, chanB) = InMemoryTransportChannel.CreateConnectedPair("NodeA", "NodeB", _clusterId);
        _disposables.Add(chanA);
        _disposables.Add(chanB);

        using var storeA = new MemoryStateStore();
        using var storeB = new MemoryStateStore();

        string fileContent = "This file will be deleted by remote peer.";
        byte[] data = Encoding.UTF8.GetBytes(fileContent);
        var manifest = ChunkFingerprinter.CreateManifest("doc.txt", data);

        // Initial state: both nodes have doc.txt at clock {NodeA: 1}
        var initialClock = VectorClock.Empty.Tick("NodeA");
        var metaA = new FileMetadata("doc.txt", manifest.FileSize, manifest.RootHash, DateTimeOffset.UtcNow, initialClock);
        await storeA.UpsertFileAsync(metaA, manifest.Chunks);

        string fullPathA = Path.Combine(dirA, "doc.txt");
        await File.WriteAllBytesAsync(fullPathA, data);

        // Node B deletes doc.txt with clock {NodeA: 1, NodeB: 1}
        var deletedClock = initialClock.Tick("NodeB");
        var metaBDeleted = new FileMetadata("doc.txt", 0, MerkleTreeHelper.EmptyNodeHash, DateTimeOffset.UtcNow, deletedClock, isDeleted: true, version: 2);
        await storeB.UpsertFileAsync(metaBDeleted, Array.Empty<ChunkDescriptor>());

        var wireA = new SyncWireProtocol(storeA, syncRootDirectory: dirA);
        var wireB = new SyncWireProtocol(storeB, syncRootDirectory: dirB);
        _disposables.Add(wireA);
        _disposables.Add(wireB);

        var orchA = new SyncOrchestrator(dirA, storeA, wireA, localPeerId: "NodeA");
        _disposables.Add(orchA);

        orchA.AttachChannel(chanA);
        wireB.AttachChannel(chanB);

        // Execute sync: Node A pulls changes from Node B
        bool changes = await orchA.SynchronizeAsync(chanA);

        changes.Should().BeTrue();
        File.Exists(fullPathA).Should().BeFalse("Physical file should be deleted on Node A");

        var updatedMetaA = await storeA.GetFileAsync("doc.txt");
        updatedMetaA.Should().NotBeNull();
        updatedMetaA!.IsDeleted.Should().BeTrue("File should be marked tombstone in SQLite");
    }

    [Fact]
    public async Task ConcurrentEditVsDelete_PreservesLocalModification_Test()
    {
        string dirA = Path.Combine(_tempDir, "NodeA_concurrent");
        string dirB = Path.Combine(_tempDir, "NodeB_concurrent");
        Directory.CreateDirectory(dirA);
        Directory.CreateDirectory(dirB);

        var (chanA, chanB) = InMemoryTransportChannel.CreateConnectedPair("NodeA", "NodeB", _clusterId);
        _disposables.Add(chanA);
        _disposables.Add(chanB);

        using var storeA = new MemoryStateStore();
        using var storeB = new MemoryStateStore();

        // Node A edited file with clock {NodeA: 1}
        string contentA = "Node A modified this file!";
        byte[] dataA = Encoding.UTF8.GetBytes(contentA);
        var manifestA = ChunkFingerprinter.CreateManifest("notes.txt", dataA);
        var clockA = VectorClock.Empty.Tick("NodeA");
        var metaA = new FileMetadata("notes.txt", manifestA.FileSize, manifestA.RootHash, DateTimeOffset.UtcNow, clockA);
        await storeA.UpsertFileAsync(metaA, manifestA.Chunks);

        string pathA = Path.Combine(dirA, "notes.txt");
        await File.WriteAllBytesAsync(pathA, dataA);

        // Node B deleted file with clock {NodeB: 1} (Concurrent: clockA || clockB)
        var clockB = VectorClock.Empty.Tick("NodeB");
        var metaB = new FileMetadata("notes.txt", 0, MerkleTreeHelper.EmptyNodeHash, DateTimeOffset.UtcNow, clockB, isDeleted: true, version: 1);
        await storeB.UpsertFileAsync(metaB, Array.Empty<ChunkDescriptor>());

        var wireA = new SyncWireProtocol(storeA, syncRootDirectory: dirA);
        var wireB = new SyncWireProtocol(storeB, syncRootDirectory: dirB);
        _disposables.Add(wireA);
        _disposables.Add(wireB);

        var orchA = new SyncOrchestrator(dirA, storeA, wireA, localPeerId: "NodeA");
        _disposables.Add(orchA);

        orchA.AttachChannel(chanA);
        wireB.AttachChannel(chanB);

        bool changes = await orchA.SynchronizeAsync(chanA);

        changes.Should().BeTrue();
        // Zero data loss (ADR-0001): Node A file must NOT be deleted
        File.Exists(pathA).Should().BeTrue();
        string remainingText = await File.ReadAllTextAsync(pathA);
        remainingText.Should().Be(contentA);

        var updatedMeta = await storeA.GetFileAsync("notes.txt");
        updatedMeta!.IsDeleted.Should().BeFalse();
        // Clock should now dominate both {NodeA: 1} and {NodeB: 1}
        updatedMeta.Clock.IsDominating(clockA).Should().BeTrue();
        updatedMeta.Clock.IsDominating(clockB).Should().BeTrue();
    }
}
