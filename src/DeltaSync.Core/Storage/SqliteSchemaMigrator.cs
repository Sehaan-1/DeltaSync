using Microsoft.Data.Sqlite;

namespace DeltaSync.Core.Storage;

/// <summary>
/// Manages SQLite schema migrations and table definitions for DeltaSync state persistence.
/// </summary>
public static class SqliteSchemaMigrator
{
    public const int CurrentSchemaVersion = 1;

    private const string InitialSchemaDdl = @"
        CREATE TABLE IF NOT EXISTS files (
            file_id INTEGER PRIMARY KEY AUTOINCREMENT,
            relative_path TEXT NOT NULL COLLATE NOCASE UNIQUE,
            size_bytes INTEGER NOT NULL,
            root_hash TEXT NOT NULL,
            modified_utc_ticks INTEGER NOT NULL,
            vector_clock_json TEXT NOT NULL,
            is_deleted INTEGER NOT NULL DEFAULT 0,
            version INTEGER NOT NULL DEFAULT 1,
            updated_utc_ticks INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS chunks (
            chunk_hash TEXT PRIMARY KEY,
            length INTEGER NOT NULL,
            ref_count INTEGER NOT NULL DEFAULT 1,
            first_seen_utc_ticks INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS file_chunks (
            file_id INTEGER NOT NULL REFERENCES files(file_id) ON DELETE CASCADE,
            chunk_index INTEGER NOT NULL,
            chunk_hash TEXT NOT NULL REFERENCES chunks(chunk_hash),
            offset INTEGER NOT NULL,
            length INTEGER NOT NULL,
            PRIMARY KEY(file_id, chunk_index)
        );

        CREATE INDEX IF NOT EXISTS idx_file_chunks_hash ON file_chunks(chunk_hash);

        CREATE TABLE IF NOT EXISTS merkle_nodes (
            prefix TEXT PRIMARY KEY COLLATE NOCASE,
            node_hash TEXT NOT NULL,
            child_count INTEGER NOT NULL,
            updated_utc_ticks INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS transfer_checkpoints (
            transfer_id TEXT PRIMARY KEY,
            relative_path TEXT NOT NULL COLLATE NOCASE,
            target_root_hash TEXT NOT NULL,
            total_chunks INTEGER NOT NULL,
            completed_chunks INTEGER NOT NULL,
            temp_file_path TEXT NOT NULL,
            chunk_bitmap BLOB NOT NULL,
            updated_utc_ticks INTEGER NOT NULL
        );
    ";

    /// <summary>
    /// Checks and executes any pending schema migrations asynchronously.
    /// </summary>
    public static async Task MigrateAsync(SqliteConnection connection, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);

        int currentVersion = await GetUserVersionAsync(connection, cancellationToken).ConfigureAwait(false);

        if (currentVersion < 1)
        {
            await using var tx = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await using (var cmd = connection.CreateCommand())
                {
                    cmd.Transaction = (SqliteTransaction)tx;
                    cmd.CommandText = InitialSchemaDdl;
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                await SetUserVersionAsync(connection, (SqliteTransaction)tx, 1, cancellationToken).ConfigureAwait(false);
                await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                await tx.RollbackAsync(cancellationToken).ConfigureAwait(false);
                throw;
            }
        }
    }

    /// <summary>
    /// Checks and executes any pending schema migrations synchronously.
    /// </summary>
    public static void Migrate(SqliteConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);

        int currentVersion = GetUserVersion(connection);

        if (currentVersion < 1)
        {
            using var tx = connection.BeginTransaction();
            try
            {
                using (var cmd = connection.CreateCommand())
                {
                    cmd.Transaction = tx;
                    cmd.CommandText = InitialSchemaDdl;
                    cmd.ExecuteNonQuery();
                }

                SetUserVersion(connection, tx, 1);
                tx.Commit();
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }
    }

    private static async Task<int> GetUserVersionAsync(SqliteConnection connection, CancellationToken cancellationToken)
    {
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "PRAGMA user_version;";
        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return Convert.ToInt32(result);
    }

    private static int GetUserVersion(SqliteConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "PRAGMA user_version;";
        var result = cmd.ExecuteScalar();
        return Convert.ToInt32(result);
    }

    private static async Task SetUserVersionAsync(SqliteConnection connection, SqliteTransaction tx, int version, CancellationToken cancellationToken)
    {
        await using var cmd = connection.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = $"PRAGMA user_version = {version};";
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void SetUserVersion(SqliteConnection connection, SqliteTransaction tx, int version)
    {
        using var cmd = connection.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = $"PRAGMA user_version = {version};";
        cmd.ExecuteNonQuery();
    }
}
