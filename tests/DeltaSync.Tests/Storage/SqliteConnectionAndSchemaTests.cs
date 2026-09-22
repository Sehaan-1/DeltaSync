using System.Data;
using DeltaSync.Core.Storage;
using FluentAssertions;
using Microsoft.Data.Sqlite;
using Xunit;

namespace DeltaSync.Tests.Storage;

public sealed class SqliteConnectionAndSchemaTests : IDisposable
{
    private readonly string _testDbDir;
    private readonly string _testDbPath;

    public SqliteConnectionAndSchemaTests()
    {
        _testDbDir = Path.Combine(Path.GetTempPath(), "DeltaSyncTests_" + Guid.NewGuid().ToString("N"));
        _testDbPath = Path.Combine(_testDbDir, "state.db");
    }

    public void Dispose()
    {
        DatabaseCrashRecovery.ClearPools();
        try
        {
            if (Directory.Exists(_testDbDir))
            {
                Directory.Delete(_testDbDir, recursive: true);
            }
        }
        catch
        {
            // Best effort cleanup in test temp dir
        }
    }

    [Fact]
    public async Task SqliteStateStore_SchemaInitialization_Test()
    {
        // Arrange
        ISqliteConnectionFactory factory = new SqliteConnectionFactory(_testDbPath);

        // Act
        await using var connection = await factory.OpenConnectionAsync();
        await SqliteSchemaMigrator.MigrateAsync(connection);

        // Assert tables exist
        var tables = new List<string>();
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;";
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                tables.Add(reader.GetString(0));
            }
        }

        tables.Should().Contain(new[]
        {
            "files",
            "chunks",
            "file_chunks",
            "merkle_nodes",
            "transfer_checkpoints"
        });

        // Assert secondary index idx_file_chunks_hash exists
        var indices = new List<string>();
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_file_chunks_hash';";
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                indices.Add(reader.GetString(0));
            }
        }

        indices.Should().ContainSingle().Which.Should().Be("idx_file_chunks_hash");

        // Assert schema user_version is 1
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "PRAGMA user_version;";
            var version = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            version.Should().Be(1);
        }
    }

    [Fact]
    public async Task SqliteStateStore_PragmasConfigured_Test()
    {
        // Arrange
        ISqliteConnectionFactory factory = new SqliteConnectionFactory(_testDbPath);

        // Act
        await using var connection = await factory.OpenConnectionAsync();
        await SqliteSchemaMigrator.MigrateAsync(connection);

        // Assert journal_mode == "wal"
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "PRAGMA journal_mode;";
            var journalMode = (string?)(await cmd.ExecuteScalarAsync());
            journalMode?.ToLowerInvariant().Should().Be("wal");
        }

        // Assert synchronous == 1 (NORMAL)
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "PRAGMA synchronous;";
            var synchronous = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            synchronous.Should().Be(1, "synchronous=NORMAL is 1 in SQLite");
        }

        // Assert foreign_keys == 1 (ON)
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "PRAGMA foreign_keys;";
            var foreignKeys = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            foreignKeys.Should().Be(1, "foreign_keys=ON is 1 in SQLite");
        }

        // Assert busy_timeout == 5000
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "PRAGMA busy_timeout;";
            var busyTimeout = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            busyTimeout.Should().Be(5000);
        }
    }

    [Fact]
    public async Task Database_CrashSafety_IntegrityCheck_Test()
    {
        // Arrange
        ISqliteConnectionFactory factory = new SqliteConnectionFactory(_testDbPath);

        // Initialize schema
        await using (var initConn = await factory.OpenConnectionAsync())
        {
            await SqliteSchemaMigrator.MigrateAsync(initConn);
        }

        // Act: Begin transaction, insert 1,000 files and 10,000 chunks, dispose transaction without commit (simulating crash)
        await using (var writeConn = await factory.OpenConnectionAsync())
        {
            await using (var tx = writeConn.BeginTransaction())
            {
                // Insert 1,000 files
                for (int i = 0; i < 1000; i++)
                {
                    await using var cmdFile = writeConn.CreateCommand();
                    cmdFile.Transaction = tx;
                    cmdFile.CommandText = @"
                        INSERT INTO files (relative_path, size_bytes, root_hash, modified_utc_ticks, vector_clock_json, is_deleted, version, updated_utc_ticks)
                        VALUES ($path, $size, $hash, $mod, $vc, 0, 1, $now);";
                    cmdFile.Parameters.AddWithValue("$path", $"docs/file_{i}.txt");
                    cmdFile.Parameters.AddWithValue("$size", 1024L);
                    cmdFile.Parameters.AddWithValue("$hash", new string('a', 64));
                    cmdFile.Parameters.AddWithValue("$mod", DateTime.UtcNow.Ticks);
                    cmdFile.Parameters.AddWithValue("$vc", "{}");
                    cmdFile.Parameters.AddWithValue("$now", DateTime.UtcNow.Ticks);
                    await cmdFile.ExecuteNonQueryAsync();
                }

                // Insert 10,000 chunks
                for (int j = 0; j < 10000; j++)
                {
                    await using var cmdChunk = writeConn.CreateCommand();
                    cmdChunk.Transaction = tx;
                    cmdChunk.CommandText = @"
                        INSERT INTO chunks (chunk_hash, length, ref_count, first_seen_utc_ticks)
                        VALUES ($hash, $len, 1, $now);";
                    cmdChunk.Parameters.AddWithValue("$hash", j.ToString("x64"));
                    cmdChunk.Parameters.AddWithValue("$len", 1024);
                    cmdChunk.Parameters.AddWithValue("$now", DateTime.UtcNow.Ticks);
                    await cmdChunk.ExecuteNonQueryAsync();
                }

                // INTENTIONAL SIMULATED CRASH:
                // No tx.CommitAsync() called.
                // Disposing tx rolls back uncommitted frames.
            }
        }

        // Reopen database on fresh connection
        await using var verifyConn = await factory.OpenConnectionAsync();

        // Verify 0 files exist
        await using (var countCmd = verifyConn.CreateCommand())
        {
            countCmd.CommandText = "SELECT COUNT(*) FROM files;";
            var fileCount = Convert.ToInt64(await countCmd.ExecuteScalarAsync());
            fileCount.Should().Be(0, "uncommitted files must be rolled back after simulated crash");
        }

        // Verify 0 chunks exist
        await using (var countCmd = verifyConn.CreateCommand())
        {
            countCmd.CommandText = "SELECT COUNT(*) FROM chunks;";
            var chunkCount = Convert.ToInt64(await countCmd.ExecuteScalarAsync());
            chunkCount.Should().Be(0, "uncommitted chunks must be rolled back after simulated crash");
        }

        // Verify PRAGMA integrity_check returns "ok"
        var integrityResult = await DatabaseCrashRecovery.CheckIntegrityAsync(verifyConn);
        integrityResult.IsValid.Should().BeTrue();
        integrityResult.Message.Should().Be("ok");
    }

    [Fact]
    public async Task ForeignKeyConstraints_EnforceReferentialIntegrity()
    {
        // Arrange
        ISqliteConnectionFactory factory = new SqliteConnectionFactory(_testDbPath);
        await using var connection = await factory.OpenConnectionAsync();
        await SqliteSchemaMigrator.MigrateAsync(connection);

        // Act & Assert: Inserting file_chunks referencing non-existent file must throw SqliteException (FK violation)
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO file_chunks (file_id, chunk_index, chunk_hash, offset, length)
            VALUES (99999, 0, 'dummyhash', 0, 1024);";

        Func<Task> act = async () => await cmd.ExecuteNonQueryAsync();
        await act.Should().ThrowAsync<SqliteException>()
            .Where(ex => ex.SqliteErrorCode == 19, "SQLite Error 19 corresponds to SQLITE_CONSTRAINT foreign key failure");
    }

    [Fact]
    public async Task CascadeDelete_DeletesAssociatedFileChunks()
    {
        // Arrange
        ISqliteConnectionFactory factory = new SqliteConnectionFactory(_testDbPath);
        await using var connection = await factory.OpenConnectionAsync();
        await SqliteSchemaMigrator.MigrateAsync(connection);

        // Insert chunk
        string chunkHash = new string('c', 64);
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO chunks (chunk_hash, length, ref_count, first_seen_utc_ticks) VALUES ($hash, 1024, 1, 100);";
            cmd.Parameters.AddWithValue("$hash", chunkHash);
            await cmd.ExecuteNonQueryAsync();
        }

        // Insert file
        long fileId;
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO files (relative_path, size_bytes, root_hash, modified_utc_ticks, vector_clock_json, is_deleted, version, updated_utc_ticks)
                VALUES ('test.txt', 1024, 'roothash', 100, '{}', 0, 1, 100);
                SELECT last_insert_rowid();";
            fileId = Convert.ToInt64(await cmd.ExecuteScalarAsync());
        }

        // Insert file_chunk mapping
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO file_chunks (file_id, chunk_index, chunk_hash, offset, length) VALUES ($fid, 0, $hash, 0, 1024);";
            cmd.Parameters.AddWithValue("$fid", fileId);
            cmd.Parameters.AddWithValue("$hash", chunkHash);
            await cmd.ExecuteNonQueryAsync();
        }

        // Delete file
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "DELETE FROM files WHERE file_id = $fid;";
            cmd.Parameters.AddWithValue("$fid", fileId);
            await cmd.ExecuteNonQueryAsync();
        }

        // Verify file_chunks is automatically deleted via CASCADE
        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM file_chunks WHERE file_id = $fid;";
            cmd.Parameters.AddWithValue("$fid", fileId);
            var remaining = Convert.ToInt64(await cmd.ExecuteScalarAsync());
            remaining.Should().Be(0, "cascade delete must purge file_chunks");
        }
    }

    [Fact]
    public async Task SchemaMigration_IsIdempotent()
    {
        // Arrange
        ISqliteConnectionFactory factory = new SqliteConnectionFactory(_testDbPath);
        await using var connection = await factory.OpenConnectionAsync();

        // Act: Run twice
        await SqliteSchemaMigrator.MigrateAsync(connection);
        await SqliteSchemaMigrator.MigrateAsync(connection);

        // Assert: user_version is 1 and integrity is ok
        var integrity = await DatabaseCrashRecovery.CheckIntegrityAsync(connection);
        integrity.IsValid.Should().BeTrue();
    }

    [Fact]
    public void SynchronousApi_OpensConnection_AndMigratesSchema()
    {
        // Arrange
        ISqliteConnectionFactory factory = new SqliteConnectionFactory(_testDbPath);

        // Act
        using var connection = factory.OpenConnection();
        SqliteSchemaMigrator.Migrate(connection);

        // Assert
        var integrity = DatabaseCrashRecovery.CheckIntegrity(connection);
        integrity.IsValid.Should().BeTrue();
        integrity.Message.Should().Be("ok");
    }

    [Fact]
    public async Task Factory_CreatesNestedDirectoriesAutomatically()
    {
        // Arrange: deep nested path
        string deepPath = Path.Combine(_testDbDir, "sub1", "sub2", "sub3", "state.db");
        ISqliteConnectionFactory factory = new SqliteConnectionFactory(deepPath);

        // Act
        await using var connection = await factory.OpenConnectionAsync();
        await SqliteSchemaMigrator.MigrateAsync(connection);

        // Assert
        File.Exists(deepPath).Should().BeTrue();
    }

    [Fact]
    public async Task QuarantineCorruptedDatabase_QuarantinesFilesAndAllowsFreshDatabase()
    {
        // Arrange
        ISqliteConnectionFactory factory = new SqliteConnectionFactory(_testDbPath);
        await using (var connection = await factory.OpenConnectionAsync())
        {
            await SqliteSchemaMigrator.MigrateAsync(connection);
        }

        // Act: Quarantine the database
        DatabaseCrashRecovery.QuarantineCorruptedDatabase(_testDbPath, ".corrupt");

        // Assert: original file moved
        File.Exists(_testDbPath).Should().BeFalse();

        // Fresh database can be opened and initialized
        await using (var freshConn = await factory.OpenConnectionAsync())
        {
            await SqliteSchemaMigrator.MigrateAsync(freshConn);
            var integrity = await DatabaseCrashRecovery.CheckIntegrityAsync(freshConn);
            integrity.IsValid.Should().BeTrue();
        }
    }
}
