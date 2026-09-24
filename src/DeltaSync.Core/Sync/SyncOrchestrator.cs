using System.Collections.Concurrent;
using System.Diagnostics;
using DeltaSync.Core.Causality;
using DeltaSync.Core.Chunking;
using DeltaSync.Core.Models;
using DeltaSync.Core.Storage;
using DeltaSync.Core.Sync.Wire;
using DeltaSync.Network;
using Microsoft.Extensions.Logging;

namespace DeltaSync.Core.Sync;

/// <summary>
/// End-to-end synchronization orchestrator binding local file watcher ingestion,
/// peer wire protocol negotiation, causality conflict resolution (ADR-0001),
/// crash-safe temporary staging (ADR-0003, ADR-0004), and atomic file commits.
/// Spec §2.2, §3 Steps 3–4.
/// </summary>
public sealed class SyncOrchestrator : ISyncOrchestrator
{
    private readonly string _syncRootDirectory;
    private readonly string _canonicalRoot;
    private readonly string _stagingDirectory;
    private readonly string _tmpDirectory;
    private readonly ISqliteStateStore _stateStore;
    private readonly ISyncWireProtocol _wireProtocol;
    private readonly IFileWatcherService? _watcherService;
    private readonly LocalFileIngestor? _ingestor;
    private readonly IPeerChannelProvider? _peerProvider;
    private readonly ISyncMetricsSink _metricsSink;
    private readonly string _localPeerId;
    private readonly TimeSpan _antiEntropyInterval;
    private readonly ILogger<SyncOrchestrator>? _logger;

    private readonly ConcurrentDictionary<IPeerTransportChannel, ChannelEntry> _channels = new();
    private Timer? _antiEntropyTimer;
    private int _isRunning;
    private int _isDisposed;
    private CancellationTokenSource? _runCts;

    private sealed class ChannelEntry
    {
        public IPeerTransportChannel Channel { get; }
        public IAsyncDisposable WireAttachment { get; }
        public SemaphoreSlim SyncLock { get; } = new(1, 1);

        public ChannelEntry(IPeerTransportChannel channel, IAsyncDisposable wireAttachment)
        {
            Channel = channel;
            WireAttachment = wireAttachment;
        }
    }

    public SyncOrchestrator(
        string syncRootDirectory,
        ISqliteStateStore stateStore,
        ISyncWireProtocol wireProtocol,
        IFileWatcherService? watcherService = null,
        LocalFileIngestor? ingestor = null,
        IPeerChannelProvider? peerProvider = null,
        ISyncMetricsSink? metricsSink = null,
        string? localPeerId = null,
        TimeSpan? antiEntropyInterval = null,
        ILogger<SyncOrchestrator>? logger = null)
    {
        if (string.IsNullOrWhiteSpace(syncRootDirectory))
            throw new ArgumentException("Sync root directory cannot be null or whitespace.", nameof(syncRootDirectory));

        _syncRootDirectory = Path.GetFullPath(syncRootDirectory);
        _canonicalRoot = _syncRootDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;
        _stagingDirectory = Path.Combine(_syncRootDirectory, ".deltasync", "staging");
        _tmpDirectory = Path.Combine(_syncRootDirectory, ".deltasync", "tmp");

        _stateStore = stateStore ?? throw new ArgumentNullException(nameof(stateStore));
        _wireProtocol = wireProtocol ?? throw new ArgumentNullException(nameof(wireProtocol));
        _watcherService = watcherService;
        _ingestor = ingestor;
        _peerProvider = peerProvider;
        _metricsSink = metricsSink ?? NullSyncMetricsSink.Instance;
        _logger = logger;

        _localPeerId = !string.IsNullOrWhiteSpace(localPeerId)
            ? localPeerId.Trim()
            : (!string.IsNullOrWhiteSpace(ingestor?.LocalPeerId)
                ? ingestor.LocalPeerId
                : Guid.NewGuid().ToString("N")[..8]);

        _antiEntropyInterval = antiEntropyInterval ?? TimeSpan.FromSeconds(15);
    }

    public string SyncRootDirectory => _syncRootDirectory;
    public string LocalPeerId => _localPeerId;
    public bool IsRunning => Volatile.Read(ref _isRunning) == 1;
    public IReadOnlyCollection<IPeerTransportChannel> ConnectedChannels => _channels.Keys.ToList();

    public Task StartAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) == 1, this);

        if (Interlocked.CompareExchange(ref _isRunning, 1, 0) != 0)
        {
            return Task.CompletedTask;
        }

        _runCts = new CancellationTokenSource();

        // 1. Crash recovery: sweep any abandoned .tmp files in staging directories
        // H-06: on startup, only sweep files older than 60s to avoid clobbering active transfers
        CleanupStagingFiles(TimeSpan.FromSeconds(60));

        // 2. Wire local file watcher events
        if (_watcherService != null)
        {
            _watcherService.OnFileCreatedOrChanged += HandleLocalFileChangedAsync;
            _watcherService.OnFileDeleted += HandleLocalFileDeletedAsync;
            _watcherService.OnFileRenamed += HandleLocalFileRenamedAsync;
            _watcherService.StartWatching();
        }

        // 3. Wire peer channel provider events and attach current active connections
        if (_peerProvider != null)
        {
            _peerProvider.ConnectionEstablished += HandlePeerConnectionEstablished;
            _peerProvider.ConnectionClosed += HandlePeerConnectionClosed;

            foreach (var ch in _peerProvider.ActiveConnections)
            {
                AttachChannel(ch);
            }
        }

        // 4. Start periodic anti-entropy heartbeat timer
        if (_antiEntropyInterval > TimeSpan.Zero)
        {
            _antiEntropyTimer = new Timer(
                OnAntiEntropyHeartbeat,
                null,
                _antiEntropyInterval,
                _antiEntropyInterval);
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken ct = default)
    {
        if (Interlocked.CompareExchange(ref _isRunning, 0, 1) != 1)
        {
            return;
        }

        _runCts?.Cancel();

        // L-01: Concurrency note — StopAsync sets _isRunning = 0 via Interlocked.CompareExchange
        // (line 146) before awaiting DisposeAsync on the timer. Any heartbeat callback that fires
        // concurrently will observe IsRunning == false and return immediately, so it cannot race
        // the DisposeAsync call below. _antiEntropyTimer is then nulled only after disposal
        // completes, making the sequence deterministic.
        if (_antiEntropyTimer != null)
        {
            await _antiEntropyTimer.DisposeAsync().ConfigureAwait(false);
            _antiEntropyTimer = null;
        }

        if (_watcherService != null)
        {
            _watcherService.OnFileCreatedOrChanged -= HandleLocalFileChangedAsync;
            _watcherService.OnFileDeleted -= HandleLocalFileDeletedAsync;
            _watcherService.OnFileRenamed -= HandleLocalFileRenamedAsync;
            _watcherService.StopWatching();
        }

        if (_peerProvider != null)
        {
            _peerProvider.ConnectionEstablished -= HandlePeerConnectionEstablished;
            _peerProvider.ConnectionClosed -= HandlePeerConnectionClosed;
        }

        foreach (var entry in _channels.Values)
        {
            await entry.WireAttachment.DisposeAsync().ConfigureAwait(false);
            entry.SyncLock.Dispose();
        }
        _channels.Clear();
    }

    public IAsyncDisposable AttachChannel(IPeerTransportChannel channel)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) == 1, this);
        ArgumentNullException.ThrowIfNull(channel);

        var attachment = _wireProtocol.AttachChannel(channel);
        var entry = new ChannelEntry(channel, attachment);

        _channels.AddOrUpdate(channel, entry, (key, existing) =>
        {
            // CR-01 fix: ValueTask must be awaited; schedule cleanup on thread pool so we don't block the ConcurrentDictionary factory delegate.
            _ = Task.Run(async () =>
            {
                try { await existing.WireAttachment.DisposeAsync().ConfigureAwait(false); }
                catch { /* best-effort */ }
                finally { existing.SyncLock.Dispose(); }
            });
            return entry;
        });

        // Trigger immediate anti-entropy sync upon connection.
        // M-04 fix: capture the token BEFORE entering the lambda; _runCts is nulled during
        // StopAsync and we must not substitute CancellationToken.None after that point.
        if (IsRunning && channel.IsConnected)
        {
            var syncToken = _runCts?.Token ?? CancellationToken.None;
            _ = Task.Run(async () =>
            {
                try
                {
                    await SynchronizeAsync(channel, syncToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (syncToken.IsCancellationRequested)
                {
                    // Intentional shutdown — do not mask cancellation.
                }
                catch (Exception ex)
                {
                    _logger?.LogWarning(ex, "Initial sync failed for channel {PeerId}", channel.RemotePeerId);
                }
            });
        }

        return new ChannelUnsubscriber(this, channel);
    }

    private sealed class ChannelUnsubscriber : IAsyncDisposable
    {
        private readonly SyncOrchestrator _orchestrator;
        private readonly IPeerTransportChannel _channel;
        private int _unsubscribed;

        public ChannelUnsubscriber(SyncOrchestrator orchestrator, IPeerTransportChannel channel)
        {
            _orchestrator = orchestrator;
            _channel = channel;
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.CompareExchange(ref _unsubscribed, 1, 0) == 0)
            {
                if (_orchestrator._channels.TryRemove(_channel, out var entry))
                {
                    await entry.WireAttachment.DisposeAsync().ConfigureAwait(false);
                    entry.SyncLock.Dispose();
                }
            }
        }
    }

    public async Task<bool> SynchronizeAsync(IPeerTransportChannel channel, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) == 1, this);
        ArgumentNullException.ThrowIfNull(channel);

        if (!channel.IsConnected)
        {
            return false;
        }

        if (!_channels.TryGetValue(channel, out var entry))
        {
            // Auto-attach if not yet explicitly registered
            AttachChannel(channel);
            if (!_channels.TryGetValue(channel, out entry))
            {
                return false;
            }
        }

        // Mutual exclusion per peer channel to prevent overlapping sync cycles
        await entry.SyncLock.WaitAsync(ct).ConfigureAwait(false);
        var stopwatch = Stopwatch.StartNew();
        try
        {
            // 1. Merkle Root Probe (O(1) equality check)
            bool rootsMatch = await _wireProtocol.ProbeRootEqualityAsync(channel, ct).ConfigureAwait(false);
            if (rootsMatch)
            {
                _metricsSink.RecordSyncCycleCompleted(stopwatch.Elapsed);
                return false;
            }

            // 2. Hierarchical Prefix Divergence Traversal (O(log N))
            var divergentFiles = await _wireProtocol.ReconcilePrefixAsync(channel, "", ct).ConfigureAwait(false);
            if (divergentFiles.Count == 0)
            {
                _metricsSink.RecordSyncCycleCompleted(stopwatch.Elapsed);
                return false;
            }

            bool anyChangesApplied = false;

            // Ensure staging directory exists
            Directory.CreateDirectory(_stagingDirectory);

            foreach (var summary in divergentFiles)
            {
                ct.ThrowIfCancellationRequested();

                string normalizedPath = FileManifest.NormalizePath(summary.RelativePath);

                // Case A: Remote indicates deletion (tombstone)
                if (summary.RemoteIsDeleted)
                {
                    bool applied = await HandleRemoteDeletionAsync(channel, summary, normalizedPath, ct).ConfigureAwait(false);
                    if (applied) anyChangesApplied = true;
                    continue;
                }

                // Case B: Remote has active file content
                bool modified = await HandleRemoteFileContentAsync(channel, summary, normalizedPath, ct).ConfigureAwait(false);
                if (modified) anyChangesApplied = true;
            }

            _metricsSink.RecordSyncCycleCompleted(stopwatch.Elapsed);
            return anyChangesApplied;
        }
        finally
        {
            entry.SyncLock.Release();
        }
    }

    public async Task SynchronizeAllAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) == 1, this);

        var activeChannels = _channels.Keys.Where(ch => ch.IsConnected).ToList();
        if (activeChannels.Count == 0)
        {
            return;
        }

        var tasks = activeChannels.Select(async ch =>
        {
            try
            {
                await SynchronizeAsync(ch, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // H-04 fix: propagate intentional shutdown; do not mask cancellation.
                throw;
            }
            catch (Exception ex)
            {
                // Isolate individual channel failures — log at warning level.
                _logger?.LogWarning(ex, "Sync failed for channel {PeerId}", ch.RemotePeerId);
            }
        });

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    public int CleanupStagingFiles() => CleanupStagingFiles(null);

    public int CleanupStagingFiles(TimeSpan? olderThan)
    {
        int deletedCount = 0;
        var directoriesToSweep = new[] { _stagingDirectory, _tmpDirectory };
        var cutoff = olderThan.HasValue && olderThan.Value > TimeSpan.Zero
            ? DateTime.UtcNow - olderThan.Value
            : DateTime.MaxValue;

        foreach (var dir in directoriesToSweep)
        {
            if (!Directory.Exists(dir))
            {
                continue;
            }

            try
            {
                var dirInfo = new DirectoryInfo(dir);
                foreach (var file in dirInfo.EnumerateFiles("*.tmp", SearchOption.AllDirectories))
                {
                    // H-06 fix: when an age threshold is provided (such as on startup), only delete
                    // files older than the threshold to avoid clobbering an active concurrent transfer.
                    if (file.LastWriteTimeUtc > cutoff)
                        continue;
                    try
                    {
                        file.Delete();
                        deletedCount++;
                    }
                    catch
                    {
                        // Best-effort cleanup for locked or inaccessible temp files
                    }
                }
            }
            catch
            {
                // Best-effort sweep
            }
        }

        return deletedCount;
    }

    private async Task<bool> HandleRemoteDeletionAsync(
        IPeerTransportChannel channel,
        DivergentFileSummary summary,
        string normalizedPath,
        CancellationToken ct)
    {
        var localMeta = await _stateStore.GetFileAsync(normalizedPath, ct).ConfigureAwait(false);
        if (localMeta == null || localMeta.IsDeleted)
        {
            return false;
        }

        var localClock = localMeta.Clock ?? VectorClock.Empty;
        var remoteClock = summary.RemoteClock;
        bool isRemoteDeleted = summary.RemoteIsDeleted;

        if (remoteClock == null)
        {
            var remoteManifest = await _wireProtocol.FetchManifestAsync(channel, normalizedPath, ct).ConfigureAwait(false);
            if (remoteManifest != null)
            {
                remoteClock = remoteManifest.Clock;
                isRemoteDeleted = remoteManifest.IsDeleted;
            }
        }

        remoteClock ??= VectorClock.Empty;

        // If remote never had this file and didn't delete it, local retains it (remote will pull)
        if (!isRemoteDeleted && remoteClock == VectorClock.Empty)
        {
            return false;
        }

        var relation = localClock.Compare(remoteClock);

        // Remote deletion happened strictly after local modification (VA < VB)
        if (relation == CausalRelation.Before)
        {
            _watcherService?.SuppressPath(normalizedPath);
            try
            {
                string fullPath = Path.GetFullPath(Path.Combine(_syncRootDirectory, normalizedPath));
                if (File.Exists(fullPath))
                {
                    File.Delete(fullPath);
                }

                await _stateStore.DeleteFileAsync(normalizedPath, ct).ConfigureAwait(false);
                return true;
            }
            finally
            {
                _watcherService?.UnsuppressPath(normalizedPath);
            }
        }

        // Local modification happened after remote deletion (VB < VA): local retains file
        if (relation == CausalRelation.After)
        {
            return false;
        }

        // Concurrent edit vs delete: ADR-0001 guarantees zero silent data loss.
        // Retain local content, advance local clock to dominate deletion tombstone.
        var mergedClock = localClock.Merge(remoteClock).Tick(_localPeerId);
        var chunks = await _stateStore.GetFileChunksAsync(normalizedPath, ct).ConfigureAwait(false);
        var updated = localMeta with { Clock = mergedClock, Version = localMeta.Version + 1 };
        await _stateStore.UpsertFileAsync(updated, chunks, ct).ConfigureAwait(false);
        return true;
    }

    private async Task<bool> HandleRemoteFileContentAsync(
        IPeerTransportChannel channel,
        DivergentFileSummary summary,
        string normalizedPath,
        CancellationToken ct)
    {
        // 1. Fetch remote manifest containing chunk fingerprints and remote vector clock
        var remoteManifest = await _wireProtocol.FetchManifestAsync(channel, normalizedPath, ct).ConfigureAwait(false);
        if (remoteManifest == null || string.IsNullOrEmpty(remoteManifest.ContentHash))
        {
            return false;
        }

        // 2. Query local file state
        var localMeta = await _stateStore.GetFileAsync(normalizedPath, ct).ConfigureAwait(false);
        string? localHash = (localMeta == null || localMeta.IsDeleted) ? null : localMeta.RootHash;
        VectorClock localClock = localMeta?.Clock ?? VectorClock.Empty;

        // 3. Causality evaluation via ConflictResolver
        var resolution = ConflictResolver.Resolve(
            localPeerId: _localPeerId,
            relativePath: normalizedPath,
            localHash: localHash,
            localClock: localClock,
            remotePeerId: channel.RemotePeerId,
            remoteHash: remoteManifest.ContentHash,
            remoteClock: remoteManifest.Clock,
            pathExists: p => File.Exists(Path.Combine(_syncRootDirectory, p)));

        switch (resolution.Type)
        {
            case ConflictResolutionType.NoOp:
            case ConflictResolutionType.RejectObsolete:
                return false;

            case ConflictResolutionType.MergeIdentical:
                // Content is identical; merge vector clocks component-wise in state store
                if (localMeta != null)
                {
                    var existingChunks = await _stateStore.GetFileChunksAsync(normalizedPath, ct).ConfigureAwait(false);
                    var updated = localMeta with { Clock = resolution.PrimaryVector, Version = localMeta.Version + 1 };
                    await _stateStore.UpsertFileAsync(updated, existingChunks, ct).ConfigureAwait(false);
                    return true;
                }
                return false;

            case ConflictResolutionType.ApplyRemote:
                return await ApplyRemoteFileAsync(channel, remoteManifest, normalizedPath, localMeta, resolution.PrimaryVector, ct).ConfigureAwait(false);

            case ConflictResolutionType.PreserveSideBySide:
                return await PreserveSideBySideConflictAsync(channel, remoteManifest, normalizedPath, localMeta, resolution, ct).ConfigureAwait(false);

            default:
                return false;
        }
    }

    private async Task<bool> ApplyRemoteFileAsync(
        IPeerTransportChannel channel,
        FileManifestResponse remoteManifest,
        string normalizedPath,
        FileMetadata? localMeta,
        VectorClock targetClock,
        CancellationToken ct)
    {
        string destinationFilePath = Path.GetFullPath(Path.Combine(_syncRootDirectory, normalizedPath));
        // H-01 fix: Linux filesystems are case-sensitive; use platform-appropriate comparison.
        var pathComparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;
        if (!destinationFilePath.StartsWith(_canonicalRoot, pathComparison))
        {
            throw new InvalidOperationException($"Security boundary violation: '{normalizedPath}' resolves outside sync root.");
        }

        _watcherService?.SuppressPath(normalizedPath);
        try
        {
            // Fetch missing chunks, assemble in .deltasync/staging/<guid>.tmp, verify SHA-256, and atomic move
            await _wireProtocol.FetchAndReconstructFileAsync(
                channel,
                remoteManifest,
                destinationFilePath,
                _stagingDirectory,
                ct).ConfigureAwait(false);

            // Commit metadata and chunk mapping to SQLite.
            // M-01 fix: preserve the remote file's original mtime from the manifest so that
            // FileWatcherService.FlushAsync does not repeatedly re-ingest already-synced files.
            var chunkDescriptors = remoteManifest.Chunks.Select(c => c.ToDescriptor()).ToList();
            var newMetadata = new FileMetadata(
                relativePath: normalizedPath,
                sizeBytes: remoteManifest.TotalBytes,
                rootHash: remoteManifest.ContentHash,
                modifiedUtc: remoteManifest.ModifiedUtc,
                clock: targetClock,
                isDeleted: false,
                version: (localMeta?.Version ?? 0) + 1);

            await _stateStore.UpsertFileAsync(newMetadata, chunkDescriptors, ct).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _watcherService?.UnsuppressPath(normalizedPath);
        }
    }

    private async Task<bool> PreserveSideBySideConflictAsync(
        IPeerTransportChannel channel,
        FileManifestResponse remoteManifest,
        string normalizedPath,
        FileMetadata? localMeta,
        ConflictResolutionResult resolution,
        CancellationToken ct)
    {
        _metricsSink.RecordConflictDetected();

        // Deterministic tie-breaking (ADR-0001 / RFC 4271):
        // Lexically lower PeerId retains the primary file path;
        // lexically higher PeerId moves/receives the conflict sibling file path.
        // This guarantees both nodes converge to identical files and identical Merkle roots without duplicate conflicts.
        bool localWinsPrimary = string.CompareOrdinal(_localPeerId, channel.RemotePeerId) < 0;
        string peerIdForSibling = localWinsPrimary ? channel.RemotePeerId : _localPeerId;
        string primaryWinningPeerId = localWinsPrimary ? _localPeerId : channel.RemotePeerId;

        VectorClock primaryClock = (localMeta?.Clock ?? VectorClock.Empty)
            .Merge(remoteManifest.Clock ?? VectorClock.Empty)
            .Tick(primaryWinningPeerId);

        string siblingRelPath = ConflictedPathHelper.GenerateConflictedPath(
            normalizedPath,
            peerIdForSibling,
            p => File.Exists(Path.Combine(_syncRootDirectory, p)) &&
                 !string.Equals(p, normalizedPath, StringComparison.OrdinalIgnoreCase));

        string siblingFullPath = Path.GetFullPath(Path.Combine(_syncRootDirectory, siblingRelPath));
        // H-01 fix: Linux filesystems are case-sensitive; use platform-appropriate comparison.
        var siblingPathComparison = OperatingSystem.IsWindows()
            ? StringComparison.OrdinalIgnoreCase
            : StringComparison.Ordinal;
        if (!siblingFullPath.StartsWith(_canonicalRoot, siblingPathComparison))
        {
            throw new InvalidOperationException($"Security boundary violation: sibling '{siblingRelPath}' resolves outside sync root.");
        }

        string primaryFullPath = Path.GetFullPath(Path.Combine(_syncRootDirectory, normalizedPath));

        if (localWinsPrimary)
        {
            // Branch 1: Local node retains primary path.
            // 1. Update primary file clock to supremum unified clock in SQLite.
            if (localMeta != null)
            {
                var existingChunks = await _stateStore.GetFileChunksAsync(normalizedPath, ct).ConfigureAwait(false);
                var updatedPrimary = localMeta with
                {
                    Clock = primaryClock,
                    Version = localMeta.Version + 1
                };
                await _stateStore.UpsertFileAsync(updatedPrimary, existingChunks, ct).ConfigureAwait(false);
            }

            // 2. Fetch remote payload into sibling path.
            _watcherService?.SuppressPath(siblingRelPath);
            try
            {
                var siblingManifest = remoteManifest with { RelativePath = siblingRelPath };
                await _wireProtocol.FetchAndReconstructFileAsync(
                    channel,
                    siblingManifest,
                    siblingFullPath,
                    _stagingDirectory,
                    ct).ConfigureAwait(false);

                var chunkDescriptors = remoteManifest.Chunks.Select(c => c.ToDescriptor()).ToList();
                // M-01 fix: use original remote mtime for the sibling conflict copy.
                var siblingMetadata = new FileMetadata(
                    relativePath: siblingRelPath,
                    sizeBytes: remoteManifest.TotalBytes,
                    rootHash: remoteManifest.ContentHash,
                    modifiedUtc: remoteManifest.ModifiedUtc,
                    clock: resolution.SiblingVector ?? remoteManifest.Clock,
                    isDeleted: false,
                    version: 1);

                await _stateStore.UpsertFileAsync(siblingMetadata, chunkDescriptors, ct).ConfigureAwait(false);
                return true;
            }
            finally
            {
                _watcherService?.UnsuppressPath(siblingRelPath);
            }
        }
        else
        {
            // Branch 2: Remote node retains primary path.
            // 1. Copy local file to sibling path with local clock (non-destructive so primaryFullPath remains readable to serve chunk requests).
            _watcherService?.SuppressPath(siblingRelPath);
            _watcherService?.SuppressPath(normalizedPath);
            try
            {
                if (File.Exists(primaryFullPath))
                {
                    string? siblingDir = Path.GetDirectoryName(siblingFullPath);
                    if (!string.IsNullOrEmpty(siblingDir)) Directory.CreateDirectory(siblingDir);
                    File.Copy(primaryFullPath, siblingFullPath, overwrite: true);
                }

                if (localMeta != null)
                {
                    var localChunks = await _stateStore.GetFileChunksAsync(normalizedPath, ct).ConfigureAwait(false);
                    var siblingMetadata = new FileMetadata(
                        relativePath: siblingRelPath,
                        sizeBytes: localMeta.SizeBytes,
                        rootHash: localMeta.RootHash,
                        modifiedUtc: localMeta.ModifiedUtc,
                        clock: localMeta.Clock ?? VectorClock.Empty,
                        isDeleted: false,
                        version: 1);
                    await _stateStore.UpsertFileAsync(siblingMetadata, localChunks, ct).ConfigureAwait(false);
                }

                // 2. Fetch remote payload into primary path with unified clock.
                await _wireProtocol.FetchAndReconstructFileAsync(
                    channel,
                    remoteManifest,
                    primaryFullPath,
                    _stagingDirectory,
                    ct).ConfigureAwait(false);

                var chunkDescriptors = remoteManifest.Chunks.Select(c => c.ToDescriptor()).ToList();
                // M-01 fix: use original remote mtime for the primary path in the conflict branch.
                var primaryMetadata = new FileMetadata(
                    relativePath: normalizedPath,
                    sizeBytes: remoteManifest.TotalBytes,
                    rootHash: remoteManifest.ContentHash,
                    modifiedUtc: remoteManifest.ModifiedUtc,
                    clock: primaryClock,
                    isDeleted: false,
                    version: (localMeta?.Version ?? 0) + 1);

                await _stateStore.UpsertFileAsync(primaryMetadata, chunkDescriptors, ct).ConfigureAwait(false);
                return true;
            }
            finally
            {
                _watcherService?.UnsuppressPath(siblingRelPath);
                _watcherService?.UnsuppressPath(normalizedPath);
            }
        }
    }

    private async Task HandleLocalFileChangedAsync(string relativePath)
    {
        if (!IsRunning) return;
        var ct = _runCts?.Token ?? CancellationToken.None;
        try
        {
            await SynchronizeAllAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Watcher sync failed for changed path {RelativePath}", relativePath);
        }
    }

    private async Task HandleLocalFileDeletedAsync(string relativePath)
    {
        if (!IsRunning) return;
        var ct = _runCts?.Token ?? CancellationToken.None;
        try
        {
            await SynchronizeAllAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Watcher sync failed for deleted path {RelativePath}", relativePath);
        }
    }

    private async Task HandleLocalFileRenamedAsync(string oldRelativePath, string newRelativePath)
    {
        if (!IsRunning) return;
        var ct = _runCts?.Token ?? CancellationToken.None;
        try
        {
            await SynchronizeAllAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Watcher sync failed for rename {OldPath} -> {NewPath}", oldRelativePath, newRelativePath);
        }
    }

    private void HandlePeerConnectionEstablished(object? sender, IPeerTransportChannel channel)
    {
        if (!IsRunning) return;
        AttachChannel(channel);
    }

    private void HandlePeerConnectionClosed(object? sender, string peerId)
    {
        var toRemove = _channels.Keys.FirstOrDefault(
            ch => string.Equals(ch.RemotePeerId, peerId, StringComparison.OrdinalIgnoreCase));
        if (toRemove != null && _channels.TryRemove(toRemove, out var entry))
        {
            // CR-02 fix: synchronous event handler cannot await ValueTask; schedule on thread pool.
            _ = Task.Run(async () =>
            {
                try { await entry.WireAttachment.DisposeAsync().ConfigureAwait(false); }
                catch { }
                finally { entry.SyncLock.Dispose(); }
            });
        }
    }

    private void OnAntiEntropyHeartbeat(object? state)
    {
        if (!IsRunning) return;

        // Capture token before entering the Task.Run lambda; _runCts is nulled during StopAsync
        // and we must not substitute CancellationToken.None after that point (M-04 companion fix).
        var ct = _runCts?.Token ?? CancellationToken.None;

        _ = Task.Run(async () =>
        {
            try
            {
                await SynchronizeAllAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // H-04 fix: intentional shutdown — do not suppress the cancellation signal.
                throw;
            }
            catch (Exception ex)
            {
                // Heartbeat failure: log at warning so failures are visible without crashing.
                _logger?.LogWarning(ex, "Anti-entropy heartbeat failed");
            }
        });
    }

    public void Dispose()
    {
        // L-01: Interlocked.CompareExchange ensures that if both Dispose() and DisposeAsync()
        // are called concurrently (e.g. two overlapping `using` scopes), only one will proceed.
        // The losing caller returns immediately without double-stopping or double-disposing.
        if (Interlocked.CompareExchange(ref _isDisposed, 1, 0) != 0)
        {
            return;
        }

        StopAsync().GetAwaiter().GetResult();
        _runCts?.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        // L-01: Same guard as Dispose() — see comment above.
        if (Interlocked.CompareExchange(ref _isDisposed, 1, 0) != 0)
        {
            return;
        }

        await StopAsync().ConfigureAwait(false);
        _runCts?.Dispose();
    }
}
