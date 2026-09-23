using System.Security.Cryptography;
using System.Text;
using DeltaSync.Core.Chunking;
using DeltaSync.Core.Models;
using DeltaSync.Core.Storage;
using DeltaSync.Core.Sync;
using FluentAssertions;
using Xunit;

namespace DeltaSync.Tests.Sync;

public sealed class LocalFileIngestorTests : IDisposable
{
    private readonly string _tempSyncDir;
    private readonly string _tempDbDir;
    private readonly string _dbPath;
    private readonly SqliteConnectionFactory _connectionFactory;
    private const string LocalPeerId = "peer-alpha";

    public LocalFileIngestorTests()
    {
        _tempSyncDir = Path.Combine(Path.GetTempPath(), "DeltaSyncIngestorTest_Sync_" + Guid.NewGuid().ToString("N"));
        _tempDbDir = Path.Combine(Path.GetTempPath(), "DeltaSyncIngestorTest_Db_" + Guid.NewGuid().ToString("N"));
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
    public async Task IngestFileAsync_NewFile_CreatesMetadataChunksAndTicksClock_Test()
    {
        // Arrange
        await using var store = await CreateStoreAsync();
        var ingestor = new LocalFileIngestor(_tempSyncDir, store, LocalPeerId);

        string relativePath = "docs/readme.txt";
        string fullPath = Path.Combine(_tempSyncDir, "docs", "readme.txt");
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
        byte[] content = Encoding.UTF8.GetBytes("Hello DeltaSync World!");
        await File.WriteAllBytesAsync(fullPath, content);
        string expectedSha256 = Convert.ToHexString(SHA256.HashData(content)).ToLowerInvariant();

        // Act
        var metadata = await ingestor.IngestFileAsync(relativePath);

        // Assert
        metadata.Should().NotBeNull();
        metadata!.RelativePath.Should().Be("docs/readme.txt");
        metadata.SizeBytes.Should().Be(content.Length);
        metadata.RootHash.Should().Be(expectedSha256);
        metadata.Version.Should().Be(1);
        metadata.Clock[LocalPeerId].Should().Be(1UL);
        metadata.IsDeleted.Should().BeFalse();

        // Verify state store query
        var stored = await store.GetFileAsync(relativePath);
        stored.Should().NotBeNull();
        stored!.RootHash.Should().Be(expectedSha256);
        stored.Clock[LocalPeerId].Should().Be(1UL);

        var chunks = await store.GetFileChunksAsync(relativePath);
        chunks.Should().NotBeEmpty();

        var rootMerkle = await store.GetMerkleNodeAsync("");
        rootMerkle.Should().NotBeNull();
        rootMerkle!.NodeHash.Should().NotBeEmpty();
    }

    [Fact]
    public async Task IngestFileAsync_FileModified_AdvancesClockAndIncrementsVersion_Test()
    {
        // Arrange
        await using var store = await CreateStoreAsync();
        var ingestor = new LocalFileIngestor(_tempSyncDir, store, LocalPeerId);

        string relativePath = "notes.txt";
        string fullPath = Path.Combine(_tempSyncDir, relativePath);
        await File.WriteAllTextAsync(fullPath, "v1 data");

        // Act 1: Initial Ingest
        var m1 = await ingestor.IngestFileAsync(relativePath);
        m1!.Version.Should().Be(1);
        m1.Clock[LocalPeerId].Should().Be(1UL);

        // Act 2: Modify and re-ingest
        await Task.Delay(20);
        await File.WriteAllTextAsync(fullPath, "v2 modified data payload");
        var m2 = await ingestor.IngestFileAsync(relativePath);

        // Assert
        m2.Should().NotBeNull();
        m2!.Version.Should().Be(2);
        m2.Clock[LocalPeerId].Should().Be(2UL);
        m2.RootHash.Should().NotBe(m1.RootHash);

        var stored = await store.GetFileAsync(relativePath);
        stored!.Version.Should().Be(2);
        stored.Clock[LocalPeerId].Should().Be(2UL);
    }

    [Fact]
    public async Task DeleteFileAsync_MarksTombstoneAndClearsChunkRefs_Test()
    {
        // Arrange
        await using var store = await CreateStoreAsync();
        var ingestor = new LocalFileIngestor(_tempSyncDir, store, LocalPeerId);

        string relativePath = "temp.txt";
        string fullPath = Path.Combine(_tempSyncDir, relativePath);
        await File.WriteAllTextAsync(fullPath, "to be deleted");
        await ingestor.IngestFileAsync(relativePath);

        // Act
        File.Delete(fullPath);
        bool deleted = await ingestor.DeleteFileAsync(relativePath);

        // Assert
        deleted.Should().BeTrue();
        var stored = await store.GetFileAsync(relativePath);
        stored.Should().NotBeNull();
        stored!.IsDeleted.Should().BeTrue();

        var chunks = await store.GetFileChunksAsync(relativePath);
        chunks.Should().BeEmpty();
    }

    [Fact]
    public async Task IngestAllAsync_ScansNestedDirectories_IngestsAllFiles_Test()
    {
        // Arrange
        await using var store = await CreateStoreAsync();
        var ingestor = new LocalFileIngestor(_tempSyncDir, store, LocalPeerId);

        Directory.CreateDirectory(Path.Combine(_tempSyncDir, "a", "b"));
        Directory.CreateDirectory(Path.Combine(_tempSyncDir, ".deltasync"));

        await File.WriteAllTextAsync(Path.Combine(_tempSyncDir, "root.txt"), "root");
        await File.WriteAllTextAsync(Path.Combine(_tempSyncDir, "a", "file1.txt"), "f1");
        await File.WriteAllTextAsync(Path.Combine(_tempSyncDir, "a", "b", "file2.txt"), "f2");
        await File.WriteAllTextAsync(Path.Combine(_tempSyncDir, ".deltasync", "ignored.txt"), "ignore");

        // Act
        int count = await ingestor.IngestAllAsync();

        // Assert
        count.Should().Be(3);
        var all = await store.GetAllFilesAsync(includeDeleted: false);
        all.Count.Should().Be(3);
        all.Select(f => f.RelativePath).Should().Contain(new[] { "root.txt", "a/file1.txt", "a/b/file2.txt" });
    }

    [Fact]
    public async Task Security_PathTraversal_ThrowsException_Test()
    {
        // Arrange
        await using var store = await CreateStoreAsync();
        var ingestor = new LocalFileIngestor(_tempSyncDir, store, LocalPeerId);

        // Act
        Func<Task> act = async () => await ingestor.IngestFileAsync("../../escape.txt");

        // Assert
        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*Security boundary violation*");
    }

    [Fact]
    public async Task ExcludedPaths_IgnoredAutomatically_Test()
    {
        // Arrange
        await using var store = await CreateStoreAsync();
        var ingestor = new LocalFileIngestor(_tempSyncDir, store, LocalPeerId);

        Directory.CreateDirectory(Path.Combine(_tempSyncDir, ".deltasync"));
        await File.WriteAllTextAsync(Path.Combine(_tempSyncDir, ".deltasync", "internal.db"), "internal");
        await File.WriteAllTextAsync(Path.Combine(_tempSyncDir, "temp.tmp"), "temp");

        // Act
        var m1 = await ingestor.IngestFileAsync(".deltasync/internal.db");
        var m2 = await ingestor.IngestFileAsync("temp.tmp");

        // Assert
        m1.Should().BeNull();
        m2.Should().BeNull();
    }
}
