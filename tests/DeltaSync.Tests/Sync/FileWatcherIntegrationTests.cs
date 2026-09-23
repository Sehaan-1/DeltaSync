using System.Security.Cryptography;
using System.Text;
using DeltaSync.Core.Storage;
using DeltaSync.Core.Sync;
using FluentAssertions;
using Xunit;

namespace DeltaSync.Tests.Sync;

public sealed class FileWatcherIntegrationTests : IDisposable
{
    private readonly string _tempSyncDir;
    private readonly string _tempDbDir;
    private readonly string _dbPath;
    private readonly SqliteConnectionFactory _connectionFactory;
    private const string LocalPeerId = "peer-integration";

    public FileWatcherIntegrationTests()
    {
        _tempSyncDir = Path.Combine(Path.GetTempPath(), "DeltaSyncIntegTest_Sync_" + Guid.NewGuid().ToString("N"));
        _tempDbDir = Path.Combine(Path.GetTempPath(), "DeltaSyncIntegTest_Db_" + Guid.NewGuid().ToString("N"));
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
    public async Task FileWatcher_RapidEditsAndRenames_UpdatesSqliteAndMerkle_Test()
    {
        // Arrange: Start store and watcher with 50ms sliding debounce
        await using var store = await CreateStoreAsync();
        await using var watcher = new FileWatcherService(
            _tempSyncDir,
            store,
            LocalPeerId,
            debounceWindow: TimeSpan.FromMilliseconds(50));

        watcher.StartWatching();

        var groundTruth = new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);
        var deletedPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var rng = new Random(42);

        // Step 1: Initial Seed of 30 files across multiple subdirectories
        for (int i = 0; i < 30; i++)
        {
            string subDir = (i % 3) switch
            {
                0 => "",
                1 => "docs",
                _ => Path.Combine("src", "modules", $"mod_{i % 2}")
            };

            string filename = $"seed_file_{i}.dat";
            string relativePath = string.IsNullOrEmpty(subDir)
                ? filename
                : Path.Combine(subDir, filename).Replace('\\', '/');

            string fullPath = Path.Combine(_tempSyncDir, relativePath);
            Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);

            byte[] content = new byte[rng.Next(256, 4096)];
            rng.NextBytes(content);

            await File.WriteAllBytesAsync(fullPath, content);
            groundTruth[relativePath] = content;
        }

        // Flush initial seed
        await Task.Delay(150);
        await watcher.FlushAsync();

        // Step 2: 100 Rapid Mutations (40 edits, 20 additions, 20 renames, 20 deletions)
        var activeKeys = groundTruth.Keys.ToList();

        // 40 Edits / Overwrites
        for (int i = 0; i < 40; i++)
        {
            string key = activeKeys[rng.Next(activeKeys.Count)];
            string fullPath = Path.Combine(_tempSyncDir, key);

            byte[] newContent = new byte[rng.Next(512, 8192)];
            rng.NextBytes(newContent);

            await File.WriteAllBytesAsync(fullPath, newContent);
            groundTruth[key] = newContent;
            await Task.Delay(2); // Rapid burst with minimal pause
        }

        // 20 Additions
        for (int i = 0; i < 20; i++)
        {
            string newRel = $"added/batch_{i % 3}/file_{i}.bin".Replace('\\', '/');
            string fullPath = Path.Combine(_tempSyncDir, newRel);
            Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);

            byte[] content = new byte[rng.Next(100, 2000)];
            rng.NextBytes(content);

            await File.WriteAllBytesAsync(fullPath, content);
            groundTruth[newRel] = content;
            activeKeys.Add(newRel);
            await Task.Delay(2);
        }

        // 20 Renames
        for (int i = 0; i < 20; i++)
        {
            if (activeKeys.Count == 0) break;
            int idx = rng.Next(activeKeys.Count);
            string oldRel = activeKeys[idx];
            string newRel = $"renamed/target_{i}_{Path.GetFileName(oldRel)}".Replace('\\', '/');

            string oldFull = Path.Combine(_tempSyncDir, oldRel);
            string newFull = Path.Combine(_tempSyncDir, newRel);
            Directory.CreateDirectory(Path.GetDirectoryName(newFull)!);

            if (File.Exists(oldFull))
            {
                File.Move(oldFull, newFull);
                var content = groundTruth[oldRel];
                groundTruth.Remove(oldRel);
                deletedPaths.Add(oldRel);

                groundTruth[newRel] = content;
                activeKeys[idx] = newRel;
            }
            await Task.Delay(2);
        }

        // 20 Deletions
        for (int i = 0; i < 20; i++)
        {
            if (activeKeys.Count == 0) break;
            int idx = rng.Next(activeKeys.Count);
            string delRel = activeKeys[idx];
            string fullPath = Path.Combine(_tempSyncDir, delRel);

            if (File.Exists(fullPath))
            {
                File.Delete(fullPath);
                groundTruth.Remove(delRel);
                deletedPaths.Add(delRel);
                activeKeys.RemoveAt(idx);
            }
            await Task.Delay(2);
        }

        // Step 3: Stabilization & Flush
        await Task.Delay(250);
        await watcher.FlushAsync();

        // Step 4: Verification against ground truth
        var storedFiles = await store.GetAllFilesAsync(includeDeleted: false);
        storedFiles.Count.Should().Be(groundTruth.Count, "active files in SQLite must match disk count exactly");

        foreach (var (relPath, expectedBytes) in groundTruth)
        {
            string fullPath = Path.Combine(_tempSyncDir, relPath);
            File.Exists(fullPath).Should().BeTrue($"file '{relPath}' must physically exist on disk");

            byte[] onDisk = await File.ReadAllBytesAsync(fullPath);
            onDisk.Should().Equal(expectedBytes, $"content on disk for '{relPath}' must match expected bytes");

            var meta = await store.GetFileAsync(relPath);
            meta.Should().NotBeNull($"metadata for '{relPath}' must exist in SQLite");
            meta!.IsDeleted.Should().BeFalse();
            meta.SizeBytes.Should().Be(expectedBytes.Length);

            string expectedSha256 = Convert.ToHexString(SHA256.HashData(expectedBytes)).ToLowerInvariant();
            meta.RootHash.Should().Be(expectedSha256, $"SHA-256 root hash in SQLite for '{relPath}' must match");
            meta.Clock[LocalPeerId].Should().BeGreaterThan(0UL);
            meta.Version.Should().BeGreaterThan(0);

            var chunks = await store.GetFileChunksAsync(relPath);
            chunks.Should().NotBeEmpty();
            chunks.Sum(c => (long)c.Length).Should().Be(expectedBytes.Length);

            // Verify chunks exist in inverted index
            var probe = await store.ProbeChunksAsync(chunks.Select(c => c.HashHex));
            probe.LocalHashes.Count.Should().Be(chunks.Count);
            probe.IsComplete.Should().BeTrue();
        }

        // Verify deleted files are tombstoned
        foreach (var delPath in deletedPaths)
        {
            var delMeta = await store.GetFileAsync(delPath);
            if (delMeta != null)
            {
                delMeta.IsDeleted.Should().BeTrue($"deleted file '{delPath}' must be marked as tombstoned");
            }
        }

        // Verify root Merkle node exists and is non-empty
        var rootMerkle = await store.GetMerkleNodeAsync("");
        rootMerkle.Should().NotBeNull("root Merkle prefix node must exist");
        rootMerkle!.NodeHash.Should().NotBeNullOrWhiteSpace();
        rootMerkle.ChildCount.Should().Be(groundTruth.Count);
    }

    [Fact]
    public async Task FileWatcher_DirectoryRename_UpdatesAllNestedFilesAndMerkle_Test()
    {
        // Arrange
        await using var store = await CreateStoreAsync();
        await using var watcher = new FileWatcherService(
            _tempSyncDir,
            store,
            LocalPeerId,
            debounceWindow: TimeSpan.FromMilliseconds(50));

        // Create directory with 3 files
        string oldDir = Path.Combine(_tempSyncDir, "projects", "alpha");
        string newDir = Path.Combine(_tempSyncDir, "projects", "beta");
        Directory.CreateDirectory(oldDir);

        await File.WriteAllTextAsync(Path.Combine(oldDir, "f1.txt"), "content f1");
        await File.WriteAllTextAsync(Path.Combine(oldDir, "f2.txt"), "content f2");
        await File.WriteAllTextAsync(Path.Combine(oldDir, "f3.txt"), "content f3");

        await watcher.Ingestor.IngestAllAsync();

        var initialFiles = await store.GetAllFilesAsync(includeDeleted: false);
        initialFiles.Count.Should().Be(3);

        watcher.StartWatching();

        // Act: Move entire directory
        Directory.Move(oldDir, newDir);
        await Task.Delay(200);
        await watcher.FlushAsync();

        // Assert
        var activeFiles = await store.GetAllFilesAsync(includeDeleted: false);
        activeFiles.Count.Should().Be(3);
        activeFiles.Select(f => f.RelativePath).Should().BeEquivalentTo(new[]
        {
            "projects/beta/f1.txt",
            "projects/beta/f2.txt",
            "projects/beta/f3.txt"
        });

        var old1 = await store.GetFileAsync("projects/alpha/f1.txt");
        old1!.IsDeleted.Should().BeTrue();

        var rootMerkle = await store.GetMerkleNodeAsync("");
        rootMerkle.Should().NotBeNull();
        rootMerkle!.ChildCount.Should().Be(3);
    }
}
