using System.Text;
using Microsoft.Data.Sqlite;

namespace DeltaSync.Core.Storage;

/// <summary>
/// Result of an integrity check on an SQLite database.
/// </summary>
public record IntegrityResult(bool IsValid, string Message);

/// <summary>
/// Utilities for verifying SQLite database crash safety, executing integrity checks,
/// managing connection pool disposal, and recovering from corruption.
/// </summary>
public static class DatabaseCrashRecovery
{
    /// <summary>
    /// Executes PRAGMA integrity_check on the provided connection asynchronously.
    /// Returns an <see cref="IntegrityResult"/> indicating whether the database is valid.
    /// </summary>
    public static async Task<IntegrityResult> CheckIntegrityAsync(SqliteConnection connection, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "PRAGMA integrity_check;";

        var sb = new StringBuilder();
        bool hasRows = false;
        bool isOk = false;

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            hasRows = true;
            string val = reader.GetString(0);
            if (val.Equals("ok", StringComparison.OrdinalIgnoreCase))
            {
                isOk = true;
                sb.Append("ok");
            }
            else
            {
                if (sb.Length > 0) sb.AppendLine();
                sb.Append(val);
            }
        }

        if (!hasRows)
        {
            return new IntegrityResult(false, "No response from PRAGMA integrity_check.");
        }

        return new IntegrityResult(isOk && sb.ToString() == "ok", sb.ToString());
    }

    /// <summary>
    /// Executes PRAGMA integrity_check on the provided connection synchronously.
    /// </summary>
    public static IntegrityResult CheckIntegrity(SqliteConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "PRAGMA integrity_check;";

        var sb = new StringBuilder();
        bool hasRows = false;
        bool isOk = false;

        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            hasRows = true;
            string val = reader.GetString(0);
            if (val.Equals("ok", StringComparison.OrdinalIgnoreCase))
            {
                isOk = true;
                sb.Append("ok");
            }
            else
            {
                if (sb.Length > 0) sb.AppendLine();
                sb.Append(val);
            }
        }

        if (!hasRows)
        {
            return new IntegrityResult(false, "No response from PRAGMA integrity_check.");
        }

        return new IntegrityResult(isOk && sb.ToString() == "ok", sb.ToString());
    }

    /// <summary>
    /// Clears all cached connections in the connection pool. Crucial on Windows before file operations.
    /// </summary>
    public static void ClearPools()
    {
        SqliteConnection.ClearAllPools();
    }

    /// <summary>
    /// Clears connection pool for a specific connection.
    /// </summary>
    public static void ClearPool(SqliteConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        SqliteConnection.ClearPool(connection);
    }

    /// <summary>
    /// Quarantines a corrupted SQLite database by clearing connection pools,
    /// renaming the file and any associated WAL/SHM files with a corrupt suffix,
    /// and allowing a fresh database to be initialized.
    /// </summary>
    public static void QuarantineCorruptedDatabase(string databasePath, string? backupSuffix = ".corrupt")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(databasePath);

        ClearPools();

        string fullPath = Path.GetFullPath(databasePath);
        string suffix = string.IsNullOrWhiteSpace(backupSuffix) ? ".corrupt" : backupSuffix;
        string timestamp = DateTime.UtcNow.ToString("yyyyMMdd_HHmmss");

        MoveFileIfExists(fullPath, $"{fullPath}{suffix}_{timestamp}");
        MoveFileIfExists($"{fullPath}-wal", $"{fullPath}-wal{suffix}_{timestamp}");
        MoveFileIfExists($"{fullPath}-shm", $"{fullPath}-shm{suffix}_{timestamp}");
    }

    private static void MoveFileIfExists(string source, string destination)
    {
        if (File.Exists(source))
        {
            try
            {
                File.Move(source, destination, overwrite: true);
            }
            catch
            {
                // Best effort quarantine
            }
        }
    }
}
