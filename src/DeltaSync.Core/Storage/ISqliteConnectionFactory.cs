using Microsoft.Data.Sqlite;

namespace DeltaSync.Core.Storage;

/// <summary>
/// Factory contract for creating and opening configured SQLite connections
/// with write-ahead logging (WAL), crash safety, and concurrency settings.
/// </summary>
public interface ISqliteConnectionFactory
{
    /// <summary>
    /// Gets the absolute filesystem path to the underlying SQLite database file.
    /// </summary>
    string DatabasePath { get; }

    /// <summary>
    /// Creates an unopened <see cref="SqliteConnection"/> with proper connection string parameters.
    /// </summary>
    SqliteConnection CreateConnection();

    /// <summary>
    /// Opens and configures a new <see cref="SqliteConnection"/> applying engine WAL pragmas.
    /// </summary>
    ValueTask<SqliteConnection> OpenConnectionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Synchronously opens and configures a new <see cref="SqliteConnection"/> applying engine WAL pragmas.
    /// </summary>
    SqliteConnection OpenConnection();
}
