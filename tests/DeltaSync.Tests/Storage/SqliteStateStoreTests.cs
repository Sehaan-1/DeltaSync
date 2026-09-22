using System.Diagnostics;
using System.Security.Cryptography;
using DeltaSync.Core.Causality;
using DeltaSync.Core.Chunking;
using DeltaSync.Core.Models;
using DeltaSync.Core.Storage;
using FluentAssertions;
using Microsoft.Data.Sqlite;
using Xunit;

namespace DeltaSync.Tests.Storage;

public sealed class SqliteStateStoreTests : IDisposable
{
    private readonly string _testDir;
    private readonly string _testDbPath;
    private readonly ISqliteConnectionFactory _connectionFactory;

    public SqliteStateStoreTests()
    {
        _testDir = Path.Combine(Path.GetTempPath(), "DeltaSyncStateStoreTests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_testDir);
        _testDbPath = Path.Combine(_testDir, "state.db");
        _connectionFactory = new SqliteConnectionFactory(_testDbPath);
    }

    public void Dispose()
    {
        DatabaseCrashRecovery.ClearPools();
        try
        {
            if (Directory.Exists(_testDir))
            {
                Directory.Delete(_testDir, recursive: true);
            }
        }
        catch
        {
            // Best effort cleanup in test temp dir
        }
    }

    private async Task<SqliteStateStore> CreateInitializedStoreAsync()
    {
        await using (var conn = await _connectionFactory.OpenConnectionAsync())
        {
            await SqliteSchemaMigrator.MigrateAsync(conn);
        }
        return new SqliteStateStore(_connectionFactory);
    }

    [Fact]
    public async Task SqliteStateStore_AtomicUpsert_Test()
    {
        // Arrange
        await using var store = await CreateInitializedStoreAsync();

        // 10 initial chunks
        var chunksA = new List<ChunkDescriptor>();
        long offset = 0;
        for (int i = 0; i < 10; i++)
        {
            byte[] hash = SHA256.HashData(BitConverter.GetBytes(i));
            chunksA.Add(new ChunkDescriptor(i, offset, 1024, hash));
            offset += 1024;
        }

        var fileA = new FileMetadata(
            "src/app.cs",
            10 * 1024,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            DateTimeOffset.UtcNow
        );

        // Act 1: Initial upsert
        await store.UpsertFileAsync(fileA, chunksA);

        // Assert 1: files has 1 row, file_chunks has 10, chunks has 10 with ref_count = 1
        await using (var conn = await _connectionFactory.OpenConnectionAsync())
        {
            await using var cmdFiles = conn.CreateCommand();
            cmdFiles.CommandText = "SELECT COUNT(*) FROM files;";
            Convert.ToInt64(await cmdFiles.ExecuteScalarAsync()).Should().Be(1);

            await using var cmdFileChunks = conn.CreateCommand();
            cmdFileChunks.CommandText = "SELECT COUNT(*) FROM file_chunks;";
            Convert.ToInt64(await cmdFileChunks.ExecuteScalarAsync()).Should().Be(10);

            await using var cmdChunks = conn.CreateCommand();
            cmdChunks.CommandText = "SELECT COUNT(*), MIN(ref_count), MAX(ref_count) FROM chunks;";
            await using var reader = await cmdChunks.ExecuteReaderAsync();
            await reader.ReadAsync();
            reader.GetInt64(0).Should().Be(10);
            reader.GetInt32(1).Should().Be(1);
            reader.GetInt32(2).Should().Be(1);
        }

        // Act 2: Update file with 8 chunks (4 retained from index 0..3, 4 new from index 100..103)
        var chunksB = new List<ChunkDescriptor>();
        offset = 0;
        for (int i = 0; i < 4; i++)
        {
            // Retained
            chunksB.Add(new ChunkDescriptor(i, offset, 1024, chunksA[i].Hash));
            offset += 1024;
        }
        for (int i = 0; i < 4; i++)
        {
            // New
            byte[] hash = SHA256.HashData(BitConverter.GetBytes(100 + i));
            chunksB.Add(new ChunkDescriptor(4 + i, offset, 1024, hash));
            offset += 1024;
        }

        var fileB = new FileMetadata(
            "src/app.cs",
            8 * 1024,
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            DateTimeOffset.UtcNow
        );

        await store.UpsertFileAsync(fileB, chunksB);

        // Assert 2: 4 orphaned chunks deleted (ref_count = 0), 4 retained have ref_count = 1, 4 new added. Total file_chunks == 8. Total chunks == 8.
        await using (var conn = await _connectionFactory.OpenConnectionAsync())
        {
            await using var cmdFiles = conn.CreateCommand();
            cmdFiles.CommandText = "SELECT COUNT(*), version FROM files WHERE relative_path = 'src/app.cs';";
            await using var readerFiles = await cmdFiles.ExecuteReaderAsync();
            await readerFiles.ReadAsync();
            readerFiles.GetInt64(0).Should().Be(1);
            readerFiles.GetInt32(1).Should().Be(2, "version must increment on update");

            await using var cmdFileChunks = conn.CreateCommand();
            cmdFileChunks.CommandText = "SELECT COUNT(*) FROM file_chunks;";
            Convert.ToInt64(await cmdFileChunks.ExecuteScalarAsync()).Should().Be(8);

            await using var cmdChunks = conn.CreateCommand();
            cmdChunks.CommandText = "SELECT COUNT(*), MIN(ref_count), MAX(ref_count) FROM chunks;";
            await using var readerChunks = await cmdChunks.ExecuteReaderAsync();
            await readerChunks.ReadAsync();
            readerChunks.GetInt64(0).Should().Be(8, "4 pruned + 4 retained + 4 new == 8 chunks");
            readerChunks.GetInt32(1).Should().Be(1);
            readerChunks.GetInt32(2).Should().Be(1);
        }

        var fetchedFile = await store.GetFileAsync("src/app.cs");
        fetchedFile.Should().NotBeNull();
        fetchedFile!.SizeBytes.Should().Be(8 * 1024);
        fetchedFile.Version.Should().Be(2);

        var fetchedChunks = await store.GetFileChunksAsync("src/app.cs");
        fetchedChunks.Should().HaveCount(8);
        fetchedChunks[0].HashHex.Should().Be(chunksB[0].HashHex);
        fetchedChunks[7].HashHex.Should().Be(chunksB[7].HashHex);
    }

    [Fact]
    public async Task SqliteStateStore_InvertedIndex_ChunkLocation_Test()
    {
        // Arrange: Write actual file to test directory with known payload
        await using var store = await CreateInitializedStoreAsync();

        string relativePath = "data/payload.bin";
        string physicalPath = Path.Combine(_testDir, relativePath.Replace('/', Path.DirectorySeparatorChar));
        Directory.CreateDirectory(Path.GetDirectoryName(physicalPath)!);

        byte[] chunk0Bytes = new byte[1024];
        byte[] chunk1Bytes = new byte[2048];
        Random.Shared.NextBytes(chunk0Bytes);
        Random.Shared.NextBytes(chunk1Bytes);

        byte[] fileBytes = new byte[chunk0Bytes.Length + chunk1Bytes.Length];
        Buffer.BlockCopy(chunk0Bytes, 0, fileBytes, 0, chunk0Bytes.Length);
        Buffer.BlockCopy(chunk1Bytes, 0, fileBytes, chunk0Bytes.Length, chunk1Bytes.Length);
        await File.WriteAllBytesAsync(physicalPath, fileBytes);

        byte[] hash0 = SHA256.HashData(chunk0Bytes);
        byte[] hash1 = SHA256.HashData(chunk1Bytes);
        byte[] rootHash = SHA256.HashData(fileBytes);

        var chunks = new List<ChunkDescriptor>
        {
            new(0, 0, chunk0Bytes.Length, hash0),
            new(1, chunk0Bytes.Length, chunk1Bytes.Length, hash1)
        };

        var metadata = new FileMetadata(
            relativePath,
            fileBytes.Length,
            Convert.ToHexString(rootHash).ToLowerInvariant(),
            DateTimeOffset.UtcNow
        );

        await store.UpsertFileAsync(metadata, chunks);

        // Act: Query chunk location via inverted index
        string chunk1HashHex = Convert.ToHexString(hash1).ToLowerInvariant();
        var location = await store.GetChunkLocationAsync(chunk1HashHex);

        // Assert location resolution
        location.Should().NotBeNull();
        location!.RelativePath.Should().Be(relativePath);
        location.Offset.Should().Be(chunk0Bytes.Length);
        location.Length.Should().Be(chunk1Bytes.Length);

        // Direct file slice read verification
        byte[] readBuffer = new byte[location.Length];
        using (var fs = new FileStream(physicalPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            fs.Seek(location.Offset, SeekOrigin.Begin);
            int bytesRead = await fs.ReadAsync(readBuffer);
            bytesRead.Should().Be(location.Length);
        }

        byte[] readHash = SHA256.HashData(readBuffer);
        readHash.Should().Equal(hash1);

        // Verify SqliteLocalChunkProvider integration
        using var provider = new SqliteLocalChunkProvider(store, _testDir);
        var providerChunk = await provider.GetChunkAsync(chunk1HashHex);
        providerChunk.Should().NotBeNull();
        providerChunk!.Value.ToArray().Should().Equal(chunk1Bytes);

        bool syncFound = provider.TryGetChunk(chunk1HashHex, out var syncChunk);
        syncFound.Should().BeTrue();
        syncChunk.ToArray().Should().Equal(chunk1Bytes);
    }

    [Fact]
    public async Task SqliteStateStore_BatchProbe_Latency_Test()
    {
        // Arrange: Seed database with 5,000 files and 50,000 chunks
        await using var store = await CreateInitializedStoreAsync();

        const int fileCount = 5000;
        const int totalChunks = 50000;
        const int chunksPerFile = totalChunks / fileCount; // 10 chunks per file

        var allInsertedChunkHashes = new List<string>(totalChunks);

        // Seed directly in a single high-speed transaction
        await using (var conn = await _connectionFactory.OpenConnectionAsync())
        {
            await using var tx = await conn.BeginTransactionAsync();

            for (int f = 0; f < fileCount; f++)
            {
                await using var cmdFile = conn.CreateCommand();
                cmdFile.Transaction = (SqliteTransaction)tx;
                cmdFile.CommandText = @"
                    INSERT INTO files (relative_path, size_bytes, root_hash, modified_utc_ticks, vector_clock_json, is_deleted, version, updated_utc_ticks)
                    VALUES ($path, $size, $hash, $mod, '{}', 0, 1, $now);
                    SELECT last_insert_rowid();";
                cmdFile.Parameters.AddWithValue("$path", $"folder_{f / 100}/file_{f}.bin");
                cmdFile.Parameters.AddWithValue("$size", chunksPerFile * 1024L);
                cmdFile.Parameters.AddWithValue("$hash", new string('a', 64));
                cmdFile.Parameters.AddWithValue("$mod", DateTime.UtcNow.Ticks);
                cmdFile.Parameters.AddWithValue("$now", DateTime.UtcNow.Ticks);
                long fileId = Convert.ToInt64(await cmdFile.ExecuteScalarAsync());

                for (int c = 0; c < chunksPerFile; c++)
                {
                    int globalChunkIdx = f * chunksPerFile + c;
                    string chunkHash = globalChunkIdx.ToString("x64");
                    allInsertedChunkHashes.Add(chunkHash);

                    await using var cmdChunk = conn.CreateCommand();
                    cmdChunk.Transaction = (SqliteTransaction)tx;
                    cmdChunk.CommandText = @"
                        INSERT INTO chunks (chunk_hash, length, ref_count, first_seen_utc_ticks)
                        VALUES ($hash, 1024, 1, $now);";
                    cmdChunk.Parameters.AddWithValue("$hash", chunkHash);
                    cmdChunk.Parameters.AddWithValue("$now", DateTime.UtcNow.Ticks);
                    await cmdChunk.ExecuteNonQueryAsync();

                    await using var cmdFc = conn.CreateCommand();
                    cmdFc.Transaction = (SqliteTransaction)tx;
                    cmdFc.CommandText = @"
                        INSERT INTO file_chunks (file_id, chunk_index, chunk_hash, offset, length)
                        VALUES ($fid, $cidx, $hash, $off, 1024);";
                    cmdFc.Parameters.AddWithValue("$fid", fileId);
                    cmdFc.Parameters.AddWithValue("$cidx", c);
                    cmdFc.Parameters.AddWithValue("$hash", chunkHash);
                    cmdFc.Parameters.AddWithValue("$off", c * 1024L);
                    await cmdFc.ExecuteNonQueryAsync();
                }
            }

            await tx.CommitAsync();
        }

        // Generate query manifest: 500 existing + 500 absent chunk hashes
        var probeHashes = new List<string>(1000);
        for (int i = 0; i < 500; i++)
        {
            probeHashes.Add(allInsertedChunkHashes[i * 10]); // sampled existing
        }
        for (int i = 0; i < 500; i++)
        {
            probeHashes.Add((totalChunks + 10000 + i).ToString("x64")); // guaranteed absent
        }

        // Act & Time
        var sw = Stopwatch.StartNew();
        var probeResult = await store.ProbeChunksAsync(probeHashes);
        sw.Stop();

        // Assert: 500 local, 500 missing, latency < 30 ms
        probeResult.LocalCount.Should().Be(500);
        probeResult.MissingCount.Should().Be(500);
        probeResult.TotalCount.Should().Be(1000);
        probeResult.IsComplete.Should().BeFalse();

        foreach (var localHash in probeHashes.Take(500))
        {
            probeResult.LocalHashes.Should().Contain(localHash);
        }
        foreach (var missingHash in probeHashes.Skip(500))
        {
            probeResult.MissingHashes.Should().Contain(missingHash);
        }

        sw.ElapsedMilliseconds.Should().BeLessThan(30, "Spec §10 Check 3 requires 1,000 chunk probe latency < 30 ms");
    }

    [Fact]
    public async Task SqliteStateStore_DeleteFile_RefCountsMaintained_Test()
    {
        // Arrange
        await using var store = await CreateInitializedStoreAsync();

        byte[] sharedHash0 = SHA256.HashData(new byte[] { 1, 2, 3 });
        byte[] sharedHash1 = SHA256.HashData(new byte[] { 4, 5, 6 });
        byte[] uniqueHashA = SHA256.HashData(new byte[] { 7, 8, 9 });
        byte[] uniqueHashB = SHA256.HashData(new byte[] { 10, 11, 12 });

        var chunksFile1 = new List<ChunkDescriptor>
        {
            new(0, 0, 1024, sharedHash0),
            new(1, 1024, 1024, sharedHash1),
            new(2, 2048, 1024, uniqueHashA)
        };

        var chunksFile2 = new List<ChunkDescriptor>
        {
            new(0, 0, 1024, sharedHash0),
            new(1, 1024, 1024, sharedHash1),
            new(2, 2048, 1024, uniqueHashB)
        };

        var file1 = new FileMetadata("file1.txt", 3072, new string('1', 64), DateTimeOffset.UtcNow);
        var file2 = new FileMetadata("file2.txt", 3072, new string('2', 64), DateTimeOffset.UtcNow);

        await store.UpsertFileAsync(file1, chunksFile1);
        await store.UpsertFileAsync(file2, chunksFile2);

        // Verify initial ref counts: shared = 2, unique = 1
        await using (var conn = await _connectionFactory.OpenConnectionAsync())
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT chunk_hash, ref_count FROM chunks;";
            await using var reader = await cmd.ExecuteReaderAsync();
            var refCounts = new Dictionary<string, int>();
            while (await reader.ReadAsync())
            {
                refCounts[reader.GetString(0)] = reader.GetInt32(1);
            }

            refCounts[Convert.ToHexString(sharedHash0).ToLowerInvariant()].Should().Be(2);
            refCounts[Convert.ToHexString(sharedHash1).ToLowerInvariant()].Should().Be(2);
            refCounts[Convert.ToHexString(uniqueHashA).ToLowerInvariant()].Should().Be(1);
            refCounts[Convert.ToHexString(uniqueHashB).ToLowerInvariant()].Should().Be(1);
        }

        // Act 1: Delete file 1
        bool deleted1 = await store.DeleteFileAsync("file1.txt");
        deleted1.Should().BeTrue();

        // Assert 1: file1 is deleted, uniqueHashA is purged, shared chunks have ref_count = 1
        var f1 = await store.GetFileAsync("file1.txt");
        f1.Should().NotBeNull();
        f1!.IsDeleted.Should().BeTrue();

        var activeFiles = await store.GetAllFilesAsync(includeDeleted: false);
        activeFiles.Should().ContainSingle().Which.RelativePath.Should().Be("file2.txt");

        await using (var conn = await _connectionFactory.OpenConnectionAsync())
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT chunk_hash, ref_count FROM chunks;";
            await using var reader = await cmd.ExecuteReaderAsync();
            var refCounts = new Dictionary<string, int>();
            while (await reader.ReadAsync())
            {
                refCounts[reader.GetString(0)] = reader.GetInt32(1);
            }

            refCounts.Should().NotContainKey(Convert.ToHexString(uniqueHashA).ToLowerInvariant());
            refCounts[Convert.ToHexString(sharedHash0).ToLowerInvariant()].Should().Be(1);
            refCounts[Convert.ToHexString(sharedHash1).ToLowerInvariant()].Should().Be(1);
            refCounts[Convert.ToHexString(uniqueHashB).ToLowerInvariant()].Should().Be(1);
        }

        // Act 2: Delete file 2
        bool deleted2 = await store.DeleteFileAsync("file2.txt");
        deleted2.Should().BeTrue();

        // Assert 2: All chunks purged
        await using (var conn = await _connectionFactory.OpenConnectionAsync())
        {
            await using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM chunks;";
            Convert.ToInt64(await cmd.ExecuteScalarAsync()).Should().Be(0);

            await using var cmdFc = conn.CreateCommand();
            cmdFc.CommandText = "SELECT COUNT(*) FROM file_chunks;";
            Convert.ToInt64(await cmdFc.ExecuteScalarAsync()).Should().Be(0);
        }
    }

    [Fact]
    public async Task SqliteStateStore_ConcurrentReadWrite_Test()
    {
        // Arrange
        await using var store = await CreateInitializedStoreAsync();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        // Background writer writing 50 files
        var writerTask = Task.Run(async () =>
        {
            for (int i = 0; i < 50; i++)
            {
                byte[] hash = SHA256.HashData(BitConverter.GetBytes(i));
                var chunks = new List<ChunkDescriptor> { new(0, 0, 1024, hash) };
                var file = new FileMetadata($"dir/file_{i}.dat", 1024, new string('f', 64), DateTimeOffset.UtcNow);
                await store.UpsertFileAsync(file, chunks);
                await Task.Delay(5);
            }
        });

        // 5 concurrent reader tasks querying GetFileAsync and ProbeChunksAsync
        var readerTasks = Enumerable.Range(0, 5).Select(readerId => Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                int target = Random.Shared.Next(0, 50);
                await store.GetFileAsync($"dir/file_{target}.dat");
                await store.ProbeChunksAsync(new[] { target.ToString("x64") });
                await Task.Delay(2);
            }
        })).ToArray();

        await writerTask;
        cts.Cancel();
        await Task.WhenAll(readerTasks);

        var allFiles = await store.GetAllFilesAsync();
        allFiles.Count.Should().Be(50);
    }

    [Fact]
    public async Task SqliteStateStore_PathHandling_Test()
    {
        // Arrange
        await using var store = await CreateInitializedStoreAsync();

        var chunks = new List<ChunkDescriptor>
        {
            new(0, 0, 100, SHA256.HashData(new byte[] { 1 }))
        };

        var file = new FileMetadata("Docs/SubFolder/Readme.md", 100, new string('e', 64), DateTimeOffset.UtcNow);
        await store.UpsertFileAsync(file, chunks);

        // Case-insensitive lookup
        var fetchedLower = await store.GetFileAsync("docs/subfolder/readme.md");
        fetchedLower.Should().NotBeNull();
        fetchedLower!.RelativePath.Should().Be("Docs/SubFolder/Readme.md");

        var fetchedUpper = await store.GetFileAsync("DOCS/SUBFOLDER/README.MD");
        fetchedUpper.Should().NotBeNull();

        // Path traversal rejection
        var badFile = new FileMetadata("../evil.txt", 100, new string('e', 64), DateTimeOffset.UtcNow);
        Func<Task> act1 = async () => await store.UpsertFileAsync(badFile, chunks);
        await act1.Should().ThrowAsync<ArgumentException>();

        Func<Task> act2 = async () => await store.GetFileAsync("a/../b.txt");
        await act2.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task SqliteStateStore_EmptyFile_Test()
    {
        // Arrange
        await using var store = await CreateInitializedStoreAsync();

        var emptyFile = new FileMetadata(
            "empty.bin",
            0,
            FileManifest.EmptyRootHash,
            DateTimeOffset.UtcNow
        );

        // Act
        await store.UpsertFileAsync(emptyFile, Array.Empty<ChunkDescriptor>());

        // Assert
        var fetched = await store.GetFileAsync("empty.bin");
        fetched.Should().NotBeNull();
        fetched!.SizeBytes.Should().Be(0);

        var chunks = await store.GetFileChunksAsync("empty.bin");
        chunks.Should().BeEmpty();
    }
}
