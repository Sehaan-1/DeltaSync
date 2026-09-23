namespace DeltaSync.Core.Sync;

/// <summary>
/// Core port interface for monitoring local filesystem events within a designated sync directory.
/// Normalizes paths to Unix-style relative paths and provides path suppression for remote writes.
/// </summary>
public interface IFileWatcherService : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Event fired when a local file is created or its contents are modified (after debouncing).
    /// Parameter is the normalized Unix-style relative path.
    /// </summary>
    event Func<string, Task>? OnFileCreatedOrChanged;

    /// <summary>
    /// Event fired when a local file is deleted (after debouncing).
    /// Parameter is the normalized Unix-style relative path.
    /// </summary>
    event Func<string, Task>? OnFileDeleted;

    /// <summary>
    /// Event fired when a local file is renamed.
    /// Parameters are (oldRelativePath, newRelativePath).
    /// </summary>
    event Func<string, string, Task>? OnFileRenamed;

    /// <summary>
    /// Gets a value indicating whether the watcher is actively listening to filesystem events.
    /// </summary>
    bool IsWatching { get; }

    /// <summary>
    /// Starts actively watching the configured directory recursively.
    /// </summary>
    void StartWatching();

    /// <summary>
    /// Stops watching the directory.
    /// </summary>
    void StopWatching();

    /// <summary>
    /// Suppresses event triggering for the given relative path (used by remote sync write operations to avoid echo loops).
    /// </summary>
    void SuppressPath(string relativePath);

    /// <summary>
    /// Removes suppression for the given relative path.
    /// </summary>
    void UnsuppressPath(string relativePath);

    /// <summary>
    /// Checks if a relative path is currently suppressed.
    /// </summary>
    bool IsPathSuppressed(string relativePath);
}
