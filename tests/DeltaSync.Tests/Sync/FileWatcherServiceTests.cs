using System.Collections.Concurrent;
using DeltaSync.Core.Models;
using DeltaSync.Core.Storage;
using DeltaSync.Core.Sync;
using FluentAssertions;
using Xunit;

namespace DeltaSync.Tests.Sync;

public sealed class FileWatcherServiceTests : IDisposable
{
    private readonly string _tempSyncDir;
    private readonly string _tempDbDir;
    private readonly string _dbPath;
    private readonly SqliteConnectionFactory _connectionFactory;
    private const string LocalPeerId = "peer-watcher";

    public FileWatcherServiceTests()
    {
        _tempSyncDir = Path.Combine(Path.GetTempPath(), "DeltaSyncWatcherTest_Sync_" + Guid.NewGuid().ToString("N"));
        _tempDbDir = Path.Combine(Path.GetTempPath(), "DeltaSyncWatcherTest_Db_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempSyncDir);
        Directory.CreateDirectory(_tempDbDir);

        _dbPath = Path.Combine(_tempDbDir, "state.db");
        _connectionFactory = new SqliteConnectionFactory(_dbPath);
    }

    public void Dispose()
    {
        DatabaseCrashRecovery.ClearPools();
        try
        {
            if (Directory.Exists(_tempSyncDir)) Directory.Delete(_tempSyncDir, recursive: true);
        }
        catch { }

        try
        {
            if (Directory.Exists(_tempDbDir)) Directory.Delete(_tempDbDir, recursive: true);
        }
        catch { }
    }

    private async Task<SqliteStateStore> CreateStoreAsync()
    {
        await using (var conn = await _connectionFactory.OpenConnectionAsync())
        {
            await SqliteSchemaMigrator.MigrateAsync(conn);
        }
        return new SqliteStateStore(_connectionFactory);
    }

    [Fact]
    public async Task FileWatcherService_StartAndStopWatching_TogglesState_Test()
    {
        await using var store = await CreateStoreAsync();
        await using var watcher = new FileWatcherService(_tempSyncDir, store, LocalPeerId);

        watcher.IsWatching.Should().BeFalse();
        watcher.StartWatching();
        watcher.IsWatching.Should().BeTrue();

        watcher.StopWatching();
        watcher.IsWatching.Should().BeFalse();
    }

    [Fact]
    public async Task FileWatcherService_RapidEdits_CoalescesIntoSingleIngestion_Test()
    {
        // Arrange
        await using var store = await CreateStoreAsync();
        // Set short debounce window for rapid test execution (100ms)
        await using var watcher = new FileWatcherService(_tempSyncDir, store, LocalPeerId, TimeSpan.FromMilliseconds(100));

        var eventCount = 0;
        watcher.OnFileCreatedOrChanged += (p) =>
        {
            Interlocked.Increment(ref eventCount);
            return Task.CompletedTask;
        };

        watcher.StartWatching();
        string filePath = Path.Combine(_tempSyncDir, "rapid.txt");

        // Act: 15 rapid writes within 50ms
        for (int i = 0; i < 15; i++)
        {
            await File.WriteAllTextAsync(filePath, $"iteration {i}");
            await Task.Delay(5);
        }

        // Wait for debounce window (100ms) + buffer to expire
        await Task.Delay(250);
        await watcher.FlushAsync();

        // Assert: Writes coalesced
        eventCount.Should().BeInRange(1, 2); // At most 1 or 2 events depending on OS FS notification timing

        var metadata = await store.GetFileAsync("rapid.txt");
        metadata.Should().NotBeNull();
        metadata!.Version.Should().BeInRange(1, 2);
        metadata.Clock[LocalPeerId].Should().Be((ulong)metadata.Version);
    }

    [Fact]
    public async Task FileWatcherService_PathSuppression_PreventsIngestion_Test()
    {
        // Arrange
        await using var store = await CreateStoreAsync();
        await using var watcher = new FileWatcherService(_tempSyncDir, store, LocalPeerId, TimeSpan.FromMilliseconds(50));

        watcher.StartWatching();
        string suppressedRel = "suppressed.txt";
        string filePath = Path.Combine(_tempSyncDir, suppressedRel);

        // Act 1: Suppress path before writing
        watcher.SuppressPath(suppressedRel);
        watcher.IsPathSuppressed(suppressedRel).Should().BeTrue();

        await File.WriteAllTextAsync(filePath, "should not ingest");
        await Task.Delay(100);
        await watcher.FlushAsync();

        // Assert 1: Store has no record of file
        var meta1 = await store.GetFileAsync(suppressedRel);
        meta1.Should().BeNull();

        // Act 2: Unsuppress and write again
        watcher.UnsuppressPath(suppressedRel);
        watcher.IsPathSuppressed(suppressedRel).Should().BeFalse();

        await File.WriteAllTextAsync(filePath, "now it should ingest");
        FileMetadata? meta2 = null;
        for (int i = 0; i < 20; i++)
        {
            await Task.Delay(50);
            await watcher.FlushAsync();
            meta2 = await store.GetFileAsync(suppressedRel);
            if (meta2 != null) break;
        }

        // Assert 2: Store now has record
        meta2.Should().NotBeNull();
        meta2!.SizeBytes.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task FileWatcherService_RenamedFile_RemovesOldAndAddsNew_Test()
    {
        // Arrange
        await using var store = await CreateStoreAsync();
        await using var watcher = new FileWatcherService(_tempSyncDir, store, LocalPeerId, TimeSpan.FromMilliseconds(50));

        string oldPath = Path.Combine(_tempSyncDir, "origin.txt");
        string newPath = Path.Combine(_tempSyncDir, "destination.txt");

        await File.WriteAllTextAsync(oldPath, "rename content payload");
        await watcher.Ingestor.IngestFileAsync("origin.txt");

        var originMeta = await store.GetFileAsync("origin.txt");
        originMeta.Should().NotBeNull();
        originMeta!.IsDeleted.Should().BeFalse();

        watcher.StartWatching();

        // Act
        File.Move(oldPath, newPath);
        FileMetadata? newMetaAfter = null;
        for (int i = 0; i < 20; i++)
        {
            await Task.Delay(50);
            await watcher.FlushAsync();
            newMetaAfter = await store.GetFileAsync("destination.txt");
            if (newMetaAfter != null && !newMetaAfter.IsDeleted) break;
        }

        // Assert
        var oldMetaAfter = await store.GetFileAsync("origin.txt");
        oldMetaAfter!.IsDeleted.Should().BeTrue();

        newMetaAfter.Should().NotBeNull();
        newMetaAfter!.IsDeleted.Should().BeFalse();
        newMetaAfter.RootHash.Should().Be(originMeta.RootHash);
    }

    [Fact]
    public async Task FlushAsync_ConcurrentTimerTick_GuaranteesAllPendingEventsProcessed()
    {
        // Arrange
        await using var store = await CreateStoreAsync();
        await using var watcher = new FileWatcherService(_tempSyncDir, store, LocalPeerId, TimeSpan.FromMilliseconds(500));

        // Enqueue 50 file events
        for (int i = 0; i < 50; i++)
        {
            string relPath = $"concurrent_{i}.dat";
            string fullPath = Path.Combine(_tempSyncDir, relPath);
            await File.WriteAllTextAsync(fullPath, $"payload data {i}");
            watcher.EnqueueFileEvent(relPath);
        }

        watcher.PendingEventCount.Should().Be(50);

        // Add micro-delay in event handler to induce lock contention
        watcher.OnFileCreatedOrChanged += async (_) =>
        {
            await Task.Yield();
        };

        // Act: Trigger concurrent background timer tick and FlushAsync()
        var tickTask = Task.Run(() => watcher.TriggerTimerTickAsync());
        var flushTask = Task.Run(() => watcher.FlushAsync());

        await Task.WhenAll(tickTask, flushTask);

        // Assert: Verify 0 pending events remain and all 50 files are accurately reconciled in SQLite
        watcher.PendingEventCount.Should().Be(0);

        var allFiles = await store.GetAllFilesAsync(includeDeleted: false);
        allFiles.Count.Should().Be(50);

        for (int i = 0; i < 50; i++)
        {
            string relPath = $"concurrent_{i}.dat";
            var meta = await store.GetFileAsync(relPath);
            meta.Should().NotBeNull($"file '{relPath}' should be indexed in SQLite");
            meta!.IsDeleted.Should().BeFalse();
            meta.SizeBytes.Should().BeGreaterThan(0);
        }
    }
}
