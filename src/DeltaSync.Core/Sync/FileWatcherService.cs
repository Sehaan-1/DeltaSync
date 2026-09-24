using System.Collections.Concurrent;
using DeltaSync.Core.Chunking;
using DeltaSync.Core.Storage;

namespace DeltaSync.Core.Sync;

/// <summary>
/// Recursive filesystem watcher that monitors a sync root directory, coalescing rapid file mutations
/// using a sliding-window debounce queue (default 500ms) before triggering the FastCDC ingestion pipeline.
/// Supports programmatic path suppression to prevent remote sync write echo loops.
/// </summary>
public sealed class FileWatcherService : IFileWatcherService
{
    private readonly string _rootDirectory;
    private readonly string _canonicalRoot;
    private readonly LocalFileIngestor _ingestor;
    private readonly TimeSpan _debounceWindow;
    private readonly ConcurrentDictionary<string, byte> _suppressedPaths = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, DebouncedFileEvent> _pendingEvents = new(StringComparer.OrdinalIgnoreCase);
    private readonly FileSystemWatcher _watcher;
    private readonly Timer _debounceTimer;
    private readonly SemaphoreSlim _processLock = new(1, 1);
    private readonly SemaphoreSlim _flushLock = new(1, 1);

    private bool _isWatching;
    // H-02 fix: use int + Interlocked.CompareExchange for atomic, race-free dispose guard.
    // (A plain bool _disposed is subject to TOCTOU if Dispose() and DisposeAsync() run concurrently.)
    private int _disposeState; // 0 = alive, 1 = disposed

    public event Func<string, Task>? OnFileCreatedOrChanged;
    public event Func<string, Task>? OnFileDeleted;
    public event Func<string, string, Task>? OnFileRenamed;

    public static readonly TimeSpan DefaultDebounceWindow = TimeSpan.FromMilliseconds(500);

    public FileWatcherService(
        string rootDirectory,
        ISqliteStateStore stateStore,
        string localPeerId,
        TimeSpan? debounceWindow = null,
        FastCdcConfig? cdcConfig = null)
        : this(rootDirectory, new LocalFileIngestor(rootDirectory, stateStore, localPeerId, cdcConfig), debounceWindow)
    {
    }

    public FileWatcherService(
        string rootDirectory,
        LocalFileIngestor ingestor,
        TimeSpan? debounceWindow = null)
    {
        if (string.IsNullOrWhiteSpace(rootDirectory))
            throw new ArgumentException("Root directory cannot be null or whitespace.", nameof(rootDirectory));

        _rootDirectory = Path.GetFullPath(rootDirectory);
        _canonicalRoot = _rootDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;

        _ingestor = ingestor ?? throw new ArgumentNullException(nameof(ingestor));
        _debounceWindow = debounceWindow ?? DefaultDebounceWindow;

        _watcher = new FileSystemWatcher(_rootDirectory)
        {
            IncludeSubdirectories = true,
            NotifyFilter = NotifyFilters.FileName |
                           NotifyFilters.DirectoryName |
                           NotifyFilters.LastWrite |
                           NotifyFilters.Size |
                           NotifyFilters.CreationTime,
            InternalBufferSize = 64 * 1024 // 64 KB buffer to avoid OS event drop during rapid bursts
        };

        _watcher.Created += (s, e) => EnqueueFileSystemEvent(e.FullPath, DebounceAction.CreatedOrChanged);
        _watcher.Changed += (s, e) => EnqueueFileSystemEvent(e.FullPath, DebounceAction.CreatedOrChanged);
        _watcher.Deleted += (s, e) => EnqueueFileSystemEvent(e.FullPath, DebounceAction.Deleted);
        _watcher.Renamed += (s, e) => EnqueueRenameEvent(e.OldFullPath, e.FullPath);
        _watcher.Error += (s, e) => HandleWatcherError(e.GetException());

        // Periodic timer checks every 25ms for expired debounce windows
        _debounceTimer = new Timer(OnTimerTick, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    public bool IsWatching => _isWatching;
    public string RootDirectory => _rootDirectory;
    public LocalFileIngestor Ingestor => _ingestor;
    public TimeSpan DebounceWindow => _debounceWindow;
    public int PendingEventCount => _pendingEvents.Count;

    public void StartWatching()
    {
        ThrowIfDisposed();
        if (_isWatching) return;

        _watcher.EnableRaisingEvents = true;
        _isWatching = true;
        _debounceTimer.Change(TimeSpan.FromMilliseconds(25), TimeSpan.FromMilliseconds(25));
    }

    public void StopWatching()
    {
        if (Volatile.Read(ref _disposeState) != 0 || !_isWatching) return;

        _watcher.EnableRaisingEvents = false;
        _isWatching = false;
        _debounceTimer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }

    public void SuppressPath(string relativePath)
    {
        if (string.IsNullOrWhiteSpace(relativePath)) return;
        string normalized = FileManifest.NormalizePath(relativePath);
        _suppressedPaths.TryAdd(normalized, 0);
    }

    public void UnsuppressPath(string relativePath)
    {
        if (string.IsNullOrWhiteSpace(relativePath)) return;
        string normalized = FileManifest.NormalizePath(relativePath);
        _suppressedPaths.TryRemove(normalized, out _);
    }

    public bool IsPathSuppressed(string relativePath)
    {
        if (string.IsNullOrWhiteSpace(relativePath)) return false;
        string normalized = FileManifest.NormalizePath(relativePath);
        return _suppressedPaths.ContainsKey(normalized);
    }

    /// <summary>
    /// Explicitly enqueues a file event for testing purposes.
    /// </summary>
    public void EnqueueFileEvent(string relativePath, bool isDeleted = false)
    {
        ThrowIfDisposed();
        string normalized = FileManifest.NormalizePath(relativePath);
        EnqueueEventInternal(normalized, isDeleted ? DebounceAction.Deleted : DebounceAction.CreatedOrChanged, null);
    }

    /// <summary>
    /// Triggers an asynchronous timer tick processing pass (for testing concurrent lock acquisition).
    /// </summary>
    public Task TriggerTimerTickAsync()
    {
        ThrowIfDisposed();
        return ProcessPendingEventsAsync(forceAll: false);
    }

    /// <summary>
    /// Flushes and processes all currently pending debounced events immediately,
    /// then performs an authoritative reconciliation against the physical filesystem
    /// to converge SQLite active records and Merkle digests.
    /// Useful for deterministic testing, graceful shutdown, and recovery after event bursts.
    /// </summary>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        await _flushLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // 1. Drain and dispatch all currently pending debounced events
            await ProcessPendingEventsAsync(forceAll: true, cancellationToken).ConfigureAwait(false);

            if (!Directory.Exists(_rootDirectory))
            {
                return;
            }

            // 2. Authoritative reconciliation: enumerate physical files currently under the sync root
            var filesOnDisk = Directory
                .EnumerateFiles(_rootDirectory, "*", SearchOption.AllDirectories)
                .Select(fullPath => Path.GetRelativePath(_rootDirectory, fullPath))
                .Where(rel => !LocalFileIngestor.IsExcludedPath(rel) && !IsPathSuppressed(rel))
                .Select(FileManifest.NormalizePath)
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            // 3. Ingest any files present on disk that are missing or out of sync in SQLite
            foreach (var relativePath in filesOnDisk)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var record = await _ingestor.StateStore.GetFileAsync(relativePath, cancellationToken).ConfigureAwait(false);
                string fullPath = Path.Combine(_rootDirectory, relativePath.Replace('/', Path.DirectorySeparatorChar));

                bool needsIngest = false;
                if (record == null || record.IsDeleted)
                {
                    needsIngest = true;
                }
                else
                {
                    var fi = new FileInfo(fullPath);
                    if (fi.Exists)
                    {
                        // M-03 fix: use a 2-second tolerance to handle FAT32/SMB timestamp granularity
                        // and prevent perpetual re-ingestion of files whose mtime differs by sub-second rounding.
                        var timeDiff = Math.Abs((fi.LastWriteTimeUtc - record.ModifiedUtc.UtcDateTime).TotalSeconds);
                        if (fi.Length != record.SizeBytes || timeDiff > 2.0)
                        {
                            needsIngest = true;
                        }
                    }
                }

                if (needsIngest)
                {
                    await _ingestor.IngestFileAsync(relativePath, cancellationToken).ConfigureAwait(false);
                }
            }

            // 4. Query active database records and mark deleted any record whose file is no longer on disk
            var activeRecords = await _ingestor.StateStore.GetAllFilesAsync(
                includeDeleted: false,
                cancellationToken).ConfigureAwait(false);

            foreach (var record in activeRecords)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var normalized = FileManifest.NormalizePath(record.RelativePath);

                if (!filesOnDisk.Contains(normalized))
                {
                    await _ingestor.DeleteFileAsync(normalized, cancellationToken).ConfigureAwait(false);
                }
            }
        }
        finally
        {
            _flushLock.Release();
        }
    }

    private void EnqueueFileSystemEvent(string fullPath, DebounceAction action)
    {
        if (Volatile.Read(ref _disposeState) != 0 || !TryResolveRelativePath(fullPath, out string? relativePath))
        {
            return;
        }

        if (LocalFileIngestor.IsExcludedPath(relativePath!) || IsPathSuppressed(relativePath!))
        {
            return;
        }

        // If it is a directory and not a file, ignore direct creation/modification
        // File events within subdirectories are captured by IncludeSubdirectories
        if (Directory.Exists(fullPath) && action != DebounceAction.Deleted)
        {
            return;
        }

        EnqueueEventInternal(relativePath!, action, null);
    }

    private void EnqueueRenameEvent(string oldFullPath, string newFullPath)
    {
        if (Volatile.Read(ref _disposeState) != 0) return;

        bool hasOld = TryResolveRelativePath(oldFullPath, out string? oldRelative);
        bool hasNew = TryResolveRelativePath(newFullPath, out string? newRelative);

        if (!hasOld && !hasNew) return;

        // If a directory was renamed, enumerate its child files and enqueue individual renames
        if (Directory.Exists(newFullPath) && hasOld && hasNew)
        {
            try
            {
                var dir = new DirectoryInfo(newFullPath);
                foreach (var file in dir.EnumerateFiles("*", SearchOption.AllDirectories))
                {
                    string childNewRel = Path.GetRelativePath(_rootDirectory, file.FullName);
                    string relSuffix = Path.GetRelativePath(newFullPath, file.FullName);
                    string childOldRel = Path.Combine(oldRelative!, relSuffix);

                    string normOld = FileManifest.NormalizePath(childOldRel);
                    string normNew = FileManifest.NormalizePath(childNewRel);

                    if (!LocalFileIngestor.IsExcludedPath(normOld) && !IsPathSuppressed(normOld))
                    {
                        EnqueueEventInternal(normOld, DebounceAction.Deleted, null);
                    }

                    if (!LocalFileIngestor.IsExcludedPath(normNew) && !IsPathSuppressed(normNew))
                    {
                        EnqueueEventInternal(normNew, DebounceAction.Renamed, normOld);
                    }
                }
                return;
            }
            catch
            {
                // Fallback to direct path handling
            }
        }

        if (hasOld)
        {
            string normOld = FileManifest.NormalizePath(oldRelative!);
            if (!LocalFileIngestor.IsExcludedPath(normOld) && !IsPathSuppressed(normOld))
            {
                EnqueueEventInternal(normOld, DebounceAction.Deleted, null);
            }
        }

        if (hasNew)
        {
            string normNew = FileManifest.NormalizePath(newRelative!);
            if (!LocalFileIngestor.IsExcludedPath(normNew) && !IsPathSuppressed(normNew))
            {
                string? normOld = hasOld ? FileManifest.NormalizePath(oldRelative!) : null;
                EnqueueEventInternal(normNew, DebounceAction.Renamed, normOld);
            }
        }
    }

    private void EnqueueEventInternal(string relativePath, DebounceAction action, string? oldRelativePath)
    {
        var deadline = DateTimeOffset.UtcNow.Add(_debounceWindow);

        _pendingEvents.AddOrUpdate(
            relativePath,
            _ => new DebouncedFileEvent(relativePath, action, deadline, oldRelativePath),
            (_, existing) =>
            {
                // Sliding window: reset deadline
                existing.Deadline = deadline;

                // Coalesce actions
                if (action == DebounceAction.Deleted)
                {
                    existing.Action = DebounceAction.Deleted;
                }
                else if (action == DebounceAction.Renamed)
                {
                    existing.Action = DebounceAction.Renamed;
                    existing.OldRelativePath = oldRelativePath;
                }
                else if (action == DebounceAction.CreatedOrChanged)
                {
                    if (existing.Action == DebounceAction.Deleted)
                    {
                        // File was deleted then recreated
                        existing.Action = DebounceAction.CreatedOrChanged;
                    }
                }

                return existing;
            });

        if (action == DebounceAction.Deleted)
        {
            string childPrefix = relativePath + "/";
            foreach (var key in _pendingEvents.Keys)
            {
                if (key.StartsWith(childPrefix, StringComparison.OrdinalIgnoreCase))
                {
                    _pendingEvents.TryRemove(key, out _);
                }
            }
        }
    }

    private void OnTimerTick(object? state)
    {
        if (Volatile.Read(ref _disposeState) != 0) return;
        _ = ProcessPendingEventsAsync(forceAll: false);
    }

    private async Task ProcessPendingEventsAsync(bool forceAll, CancellationToken cancellationToken = default)
    {
        int timeout = forceAll ? Timeout.Infinite : 0;
        if (!await _processLock.WaitAsync(timeout, cancellationToken).ConfigureAwait(false))
        {
            return;
        }

        try
        {
            var now = DateTimeOffset.UtcNow;
            var expiredKeys = new List<string>();

            foreach (var kvp in _pendingEvents)
            {
                if (forceAll || kvp.Value.Deadline <= now)
                {
                    expiredKeys.Add(kvp.Key);
                }
            }

            foreach (var key in expiredKeys)
            {
                if (!_pendingEvents.TryRemove(key, out var evt))
                {
                    continue;
                }

                if (IsPathSuppressed(evt.RelativePath))
                {
                    continue;
                }

                try
                {
                    await DispatchDebouncedEventAsync(evt, cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    // Swallowing individual dispatch errors to prevent breaking the debounce loop
                }
            }
        }
        finally
        {
            _processLock.Release();
        }
    }

    private async Task DispatchDebouncedEventAsync(DebouncedFileEvent evt, CancellationToken cancellationToken)
    {
        string fullPath = Path.Combine(_rootDirectory, evt.RelativePath);

        switch (evt.Action)
        {
            case DebounceAction.CreatedOrChanged:
                if (File.Exists(fullPath))
                {
                    await _ingestor.IngestFileAsync(evt.RelativePath, cancellationToken).ConfigureAwait(false);
                    if (OnFileCreatedOrChanged != null)
                    {
                        await OnFileCreatedOrChanged.Invoke(evt.RelativePath).ConfigureAwait(false);
                    }
                }
                else
                {
                    // File disappeared before debounce elapsed
                    await _ingestor.DeleteFileAsync(evt.RelativePath, cancellationToken).ConfigureAwait(false);
                    if (OnFileDeleted != null)
                    {
                        await OnFileDeleted.Invoke(evt.RelativePath).ConfigureAwait(false);
                    }
                }
                break;

            case DebounceAction.Deleted:
                await _ingestor.DeleteFileAsync(evt.RelativePath, cancellationToken).ConfigureAwait(false);
                if (OnFileDeleted != null)
                {
                    await OnFileDeleted.Invoke(evt.RelativePath).ConfigureAwait(false);
                }
                break;

            case DebounceAction.Renamed:
                if (!string.IsNullOrEmpty(evt.OldRelativePath))
                {
                    await _ingestor.DeleteFileAsync(evt.OldRelativePath, cancellationToken).ConfigureAwait(false);
                }

                if (File.Exists(fullPath))
                {
                    await _ingestor.IngestFileAsync(evt.RelativePath, cancellationToken).ConfigureAwait(false);
                }

                if (!string.IsNullOrEmpty(evt.OldRelativePath) && OnFileRenamed != null)
                {
                    await OnFileRenamed.Invoke(evt.OldRelativePath, evt.RelativePath).ConfigureAwait(false);
                }
                else if (OnFileCreatedOrChanged != null)
                {
                    await OnFileCreatedOrChanged.Invoke(evt.RelativePath).ConfigureAwait(false);
                }
                break;
        }
    }

    public bool TryResolveRelativePath(string fullPath, out string? relativePath, StringComparison? comparisonOverride = null)
    {
        relativePath = null;
        try
        {
            string canonical = Path.GetFullPath(fullPath);
            var comparison = comparisonOverride ?? (OperatingSystem.IsWindows()
                ? StringComparison.OrdinalIgnoreCase
                : StringComparison.Ordinal);

            if (!canonical.StartsWith(_canonicalRoot, comparison) &&
                !canonical.Equals(_rootDirectory, comparison))
            {
                return false;
            }

            relativePath = FileManifest.NormalizePath(Path.GetRelativePath(_rootDirectory, canonical));
            return true;
        }
        catch
        {
            return false;
        }
    }

    private void HandleWatcherError(Exception? ex)
    {
        // On buffer overflow or IO errors, reset watcher state
        if (ex != null && _isWatching && Volatile.Read(ref _disposeState) == 0)
        {
            try
            {
                _watcher.EnableRaisingEvents = false;
                _watcher.EnableRaisingEvents = true;
            }
            catch
            {
                // Best effort recovery
            }
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposeState) != 0)
            throw new ObjectDisposedException(GetType().FullName);
    }

    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _disposeState, 1, 0) != 0) return;

        StopWatching();
        _debounceTimer.Dispose();
        _watcher.Dispose();
        _processLock.Dispose();
        _flushLock.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _disposeState, 1, 0) != 0) return;

        StopWatching();
        await _debounceTimer.DisposeAsync().ConfigureAwait(false);
        _watcher.Dispose();
        _processLock.Dispose();
        _flushLock.Dispose();
    }

    private enum DebounceAction
    {
        CreatedOrChanged,
        Deleted,
        Renamed
    }

    private sealed class DebouncedFileEvent
    {
        public string RelativePath { get; }
        public string? OldRelativePath { get; set; }
        public DebounceAction Action { get; set; }
        public DateTimeOffset Deadline { get; set; }

        public DebouncedFileEvent(string relativePath, DebounceAction action, DateTimeOffset deadline, string? oldRelativePath = null)
        {
            RelativePath = relativePath;
            Action = action;
            Deadline = deadline;
            OldRelativePath = oldRelativePath;
        }
    }
}
