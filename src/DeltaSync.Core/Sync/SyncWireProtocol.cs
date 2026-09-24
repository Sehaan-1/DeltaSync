using System.Collections.Concurrent;
using System.Security.Cryptography;
using DeltaSync.Core.Causality;
using DeltaSync.Core.Chunking;
using DeltaSync.Core.Storage;
using DeltaSync.Core.Sync.Wire;
using DeltaSync.Network;

namespace DeltaSync.Core.Sync;

/// <summary>
/// Production implementation of ISyncWireProtocol.
/// Orchestrates Merkle anti-entropy probe exchange, hierarchical prefix difference traversal,
/// manifest queries, and sliding-window (W = 16) pipelined chunk streaming over IPeerTransportChannel.
/// Spec §2.2, §3 Steps 2–3. Honors ADR-0002 and ADR-0004.
/// </summary>
public sealed class SyncWireProtocol : ISyncWireProtocol
{
    private readonly ISqliteStateStore _stateStore;
    private readonly ILocalChunkProvider _localChunkProvider;
    private readonly string? _syncRootDirectory;
    private readonly ISyncMetricsSink _metricsSink;

    private readonly ConcurrentDictionary<IPeerTransportChannel, ChannelSession> _sessions = new();
    private long _correlationSeq;
    private int _isDisposed;

    public SyncWireProtocol(
        ISqliteStateStore stateStore,
        ILocalChunkProvider? localChunkProvider = null,
        string? syncRootDirectory = null,
        ISyncMetricsSink? metricsSink = null)
    {
        _stateStore = stateStore ?? throw new ArgumentNullException(nameof(stateStore));
        _localChunkProvider = localChunkProvider ?? new MemoryChunkProvider();
        _syncRootDirectory = syncRootDirectory;
        _metricsSink = metricsSink ?? NullSyncMetricsSink.Instance;
    }

    public IAsyncDisposable AttachChannel(IPeerTransportChannel channel)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) == 1, this);
        ArgumentNullException.ThrowIfNull(channel);

        var session = _sessions.GetOrAdd(channel, ch =>
        {
            var s = new ChannelSession(ch);
            s.StartListening(this);
            return s;
        });

        return new ChannelAttachment(this, session);
    }

    public async Task<bool> ProbeRootEqualityAsync(IPeerTransportChannel channel, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) == 1, this);
        ArgumentNullException.ThrowIfNull(channel);

        var localRoot = await _stateStore.GetMerkleNodeAsync("", ct).ConfigureAwait(false);
        string localHash = localRoot?.NodeHash ?? MerkleTreeHelper.EmptyNodeHash;

        var probe = new MerkleRootProbe(channel.ClusterId, localHash);
        var response = await SendRequestAsync<MerkleRootResponse>(
            channel,
            SyncMessageType.MerkleRootProbe,
            probe,
            ct).ConfigureAwait(false);

        return response.Matches;
    }

    public async Task<IReadOnlyList<DivergentFileSummary>> ReconcilePrefixAsync(
        IPeerTransportChannel channel,
        string prefix,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) == 1, this);
        ArgumentNullException.ThrowIfNull(channel);

        string normalizedRootPrefix = MerkleTreeHelper.NormalizePrefix(prefix);
        var queue = new Queue<string>();
        queue.Enqueue(normalizedRootPrefix);

        var divergentFiles = new List<DivergentFileSummary>();
        var visitedPrefixes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        while (queue.Count > 0)
        {
            ct.ThrowIfCancellationRequested();
            string curPrefix = queue.Dequeue();
            if (!visitedPrefixes.Add(curPrefix))
            {
                continue;
            }

            var localNode = await _stateStore.GetMerkleNodeAsync(curPrefix, ct).ConfigureAwait(false);
            var req = new PrefixDiffRequest(curPrefix, localNode?.NodeHash);

            var resp = await SendRequestAsync<PrefixDiffResponse>(
                channel,
                SyncMessageType.PrefixDiffRequest,
                req,
                ct).ConfigureAwait(false);

            if (resp.AreIdentical)
            {
                // Subtree is bit-for-bit identical; O(1) early pruning without inspecting children
                continue;
            }

            // Inspect subdirectory Merkle nodes
            if (resp.Subdirectories != null)
            {
                foreach (var sub in resp.Subdirectories)
                {
                    string subPrefix = MerkleTreeHelper.NormalizePrefix(sub.Prefix);
                    var localSub = await _stateStore.GetMerkleNodeAsync(subPrefix, ct).ConfigureAwait(false);
                    if (localSub == null || !string.Equals(localSub.NodeHash, sub.NodeHash, StringComparison.OrdinalIgnoreCase))
                    {
                        queue.Enqueue(subPrefix);
                    }
                }
            }

            // Inspect direct child files
            var remotePaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (resp.Files != null)
            {
                foreach (var remoteFile in resp.Files)
                {
                    remotePaths.Add(remoteFile.RelativePath);
                    var localFile = await _stateStore.GetFileAsync(remoteFile.RelativePath, ct).ConfigureAwait(false);
                    if (localFile == null ||
                        localFile.IsDeleted != remoteFile.IsDeleted ||
                        !string.Equals(localFile.RootHash, remoteFile.RootHash, StringComparison.OrdinalIgnoreCase))
                    {
                        divergentFiles.Add(new DivergentFileSummary(
                            remoteFile.RelativePath,
                            remoteFile.RootHash,
                            localFile?.RootHash,
                            remoteFile.SizeBytes,
                            remoteFile.Clock,
                            remoteFile.IsDeleted));
                    }
                }
            }

            // Inspect local files to identify active files that remote is missing or has deleted
            var localDiff = await _stateStore.GetDirectoryDifferenceAsync(curPrefix, "", ct).ConfigureAwait(false);
            foreach (var localFile in localDiff.Files)
            {
                if (!remotePaths.Contains(localFile.RelativePath) && !localFile.IsDeleted)
                {
                    divergentFiles.Add(new DivergentFileSummary(
                        localFile.RelativePath,
                        null,
                        localFile.RootHash,
                        0,
                        null,
                        RemoteIsDeleted: true));
                }
            }
        }

        return divergentFiles;
    }

    public async Task<FileManifestResponse> FetchManifestAsync(
        IPeerTransportChannel channel,
        string relativePath,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) == 1, this);
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentException.ThrowIfNullOrWhiteSpace(relativePath);

        var query = new FileManifestQuery(relativePath);
        return await SendRequestAsync<FileManifestResponse>(
            channel,
            SyncMessageType.FileManifestQuery,
            query,
            ct).ConfigureAwait(false);
    }

    public async Task<ReadOnlyMemory<byte>> FetchChunkAsync(
        IPeerTransportChannel channel,
        string chunkHash,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) == 1, this);
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentException.ThrowIfNullOrWhiteSpace(chunkHash);

        var req = new ChunkFetchRequest(chunkHash);
        var resp = await SendRequestAsync<ChunkPayloadResponse>(
            channel,
            SyncMessageType.ChunkFetchRequest,
            req,
            ct).ConfigureAwait(false);

        // Verify cryptographic hash integrity of chunk
        byte[] computed = SHA256.HashData(resp.Payload.Span);
        string computedHex = Convert.ToHexString(computed).ToLowerInvariant();

        if (!string.Equals(computedHex, chunkHash, StringComparison.OrdinalIgnoreCase))
        {
            throw new ChunkIntegrityException(0, chunkHash, computedHex);
        }

        _metricsSink.RecordBytesTransferred(resp.Payload.Length, isOutgoing: false);
        return resp.Payload;
    }

    public async Task<IReadOnlyDictionary<string, ReadOnlyMemory<byte>>> FetchChunksPipelinedAsync(
        IPeerTransportChannel channel,
        IReadOnlyList<string> missingChunkHashes,
        int windowSize = 16,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) == 1, this);
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentNullException.ThrowIfNull(missingChunkHashes);
        if (windowSize <= 0) throw new ArgumentOutOfRangeException(nameof(windowSize), "Window size must be > 0.");

        var results = new ConcurrentDictionary<string, ReadOnlyMemory<byte>>(StringComparer.OrdinalIgnoreCase);
        if (missingChunkHashes.Count == 0)
        {
            return results;
        }

        using var throttler = new SemaphoreSlim(windowSize, windowSize);
        var uniqueHashes = missingChunkHashes.Distinct(StringComparer.OrdinalIgnoreCase).ToList();

        var tasks = uniqueHashes.Select(async hash =>
        {
            await throttler.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                var payload = await FetchChunkAsync(channel, hash, ct).ConfigureAwait(false);
                results[hash] = payload;
            }
            finally
            {
                throttler.Release();
            }
        });

        await Task.WhenAll(tasks).ConfigureAwait(false);
        return results;
    }

    public async Task<ReconstructionResult> FetchAndReconstructFileAsync(
        IPeerTransportChannel channel,
        FileManifestResponse remoteManifest,
        string destinationFilePath,
        string? tempDirectory = null,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) == 1, this);
        ArgumentNullException.ThrowIfNull(channel);
        ArgumentNullException.ThrowIfNull(remoteManifest);
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationFilePath);

        var remoteSource = new DelegateRemoteChunkSource(async (chunkDesc, token) =>
        {
            return await FetchChunkAsync(channel, chunkDesc.HashHex, token).ConfigureAwait(false);
        });

        var manifest = remoteManifest.ToFileManifest();
        var result = await DeltaReconstructor.ReconstructAsync(
            manifest,
            destinationFilePath,
            _localChunkProvider,
            remoteSource,
            tempDirectory,
            ct).ConfigureAwait(false);

        if (remoteManifest.ModifiedUtc != default)
        {
            try
            {
                File.SetLastWriteTimeUtc(destinationFilePath, remoteManifest.ModifiedUtc.UtcDateTime);
            }
            catch
            {
                // preserve resilience on permission-restricted filesystems
            }
        }

        _metricsSink.RecordChunkDeduplicated(result.ReusedBytes);
        return result;
    }

    private ChannelSession GetOrAttachSession(IPeerTransportChannel channel)
    {
        return _sessions.GetOrAdd(channel, ch =>
        {
            var s = new ChannelSession(ch);
            s.StartListening(this);
            return s;
        });
    }

    private async Task<TResponse> SendRequestAsync<TResponse>(
        IPeerTransportChannel channel,
        SyncMessageType type,
        object request,
        CancellationToken ct)
    {
        var session = GetOrAttachSession(channel);
        ulong correlationId = (ulong)Interlocked.Increment(ref _correlationSeq);
        var tcs = new TaskCompletionSource<SyncWireFrame>(TaskCreationOptions.RunContinuationsAsynchronously);

        session.PendingRequests[correlationId] = tcs;

        using var reg = ct.Register(() => tcs.TrySetCanceled(ct));

        try
        {
            var frameBytes = SyncWireFrameSerializer.Serialize(type, correlationId, request);
            await session.SendLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                await channel.SendAsync(frameBytes, ct).ConfigureAwait(false);
                _metricsSink.RecordBytesTransferred(frameBytes.Length, isOutgoing: true);
            }
            finally
            {
                session.SendLock.Release();
            }

            var responseFrame = await tcs.Task.ConfigureAwait(false);
            return (TResponse)responseFrame.Message;
        }
        finally
        {
            session.PendingRequests.TryRemove(correlationId, out _);
        }
    }

    private async Task SendResponseAsync(
        ChannelSession session,
        SyncMessageType type,
        ulong correlationId,
        object response,
        CancellationToken ct)
    {
        var frameBytes = SyncWireFrameSerializer.Serialize(type, correlationId, response);
        await session.SendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await session.Channel.SendAsync(frameBytes, ct).ConfigureAwait(false);
            _metricsSink.RecordBytesTransferred(frameBytes.Length, isOutgoing: true);
        }
        finally
        {
            session.SendLock.Release();
        }
    }

    private async Task HandleIncomingFrameAsync(ChannelSession session, SyncWireFrame frame, CancellationToken ct)
    {
        try
        {
            switch (frame.Type)
            {
                case SyncMessageType.MerkleRootProbe:
                    var probe = (MerkleRootProbe)frame.Message;
                    var localRoot = await _stateStore.GetMerkleNodeAsync("", ct).ConfigureAwait(false);
                    string localHash = localRoot?.NodeHash ?? MerkleTreeHelper.EmptyNodeHash;
                    bool matches = string.Equals(localHash, probe.RootHash, StringComparison.OrdinalIgnoreCase);
                    var probeResp = new MerkleRootResponse(probe.ClusterId, matches, localHash);
                    await SendResponseAsync(session, SyncMessageType.MerkleRootResponse, frame.CorrelationId, probeResp, ct).ConfigureAwait(false);
                    break;

                case SyncMessageType.PrefixDiffRequest:
                    var diffReq = (PrefixDiffRequest)frame.Message;
                    var diff = await _stateStore.GetDirectoryDifferenceAsync(diffReq.Prefix, diffReq.NodeHash ?? "", ct).ConfigureAwait(false);
                    var wireFiles = diff.Files.Select(WireFileRecord.FromMetadata).ToList();
                    var wireSubdirs = diff.Subdirectories.Select(WireMerkleNodeRecord.FromNode).ToList();
                    var diffResp = new PrefixDiffResponse(diff.Prefix, diff.AreIdentical, wireFiles, wireSubdirs);
                    await SendResponseAsync(session, SyncMessageType.PrefixDiffResponse, frame.CorrelationId, diffResp, ct).ConfigureAwait(false);
                    break;

                case SyncMessageType.FileManifestQuery:
                    var query = (FileManifestQuery)frame.Message;
                    var fileMeta = await _stateStore.GetFileAsync(query.RelativePath, ct).ConfigureAwait(false);
                    if (fileMeta == null)
                    {
                        var emptyResp = new FileManifestResponse(query.RelativePath, "", 0, VectorClock.Empty, Array.Empty<WireChunkRecord>(), IsDeleted: false);
                        await SendResponseAsync(session, SyncMessageType.FileManifestResponse, frame.CorrelationId, emptyResp, ct).ConfigureAwait(false);
                    }
                    else if (fileMeta.IsDeleted)
                    {
                        var tombstoneResp = new FileManifestResponse(fileMeta.RelativePath, "", 0, fileMeta.Clock ?? VectorClock.Empty, Array.Empty<WireChunkRecord>(), IsDeleted: true);
                        await SendResponseAsync(session, SyncMessageType.FileManifestResponse, frame.CorrelationId, tombstoneResp, ct).ConfigureAwait(false);
                    }
                    else
                    {
                        var chunks = await _stateStore.GetFileChunksAsync(query.RelativePath, ct).ConfigureAwait(false);
                        var wireChunks = chunks.Select(WireChunkRecord.FromDescriptor).ToList();
                        // M-01 fix: propagate the file's original mtime so the receiver preserves it.
                        var manifestResp = new FileManifestResponse(fileMeta.RelativePath, fileMeta.RootHash, fileMeta.SizeBytes, fileMeta.Clock, wireChunks, IsDeleted: false, ModifiedUtc: fileMeta.ModifiedUtc);
                        await SendResponseAsync(session, SyncMessageType.FileManifestResponse, frame.CorrelationId, manifestResp, ct).ConfigureAwait(false);
                    }
                    break;

                case SyncMessageType.ChunkFetchRequest:
                    var chunkReq = (ChunkFetchRequest)frame.Message;
                    ReadOnlyMemory<byte> chunkPayload = default;

                    var localMem = await _localChunkProvider.GetChunkAsync(chunkReq.ChunkHash, ct).ConfigureAwait(false);
                    if (localMem.HasValue)
                    {
                        chunkPayload = localMem.Value;
                    }
                    else
                    {
                        var loc = await _stateStore.GetChunkLocationAsync(chunkReq.ChunkHash, ct).ConfigureAwait(false);
                        if (loc != null && _syncRootDirectory != null)
                        {
                            string fullPath = Path.Combine(_syncRootDirectory, loc.RelativePath);
                            if (File.Exists(fullPath))
                            {
                                using var handle = File.OpenHandle(fullPath, FileMode.Open, FileAccess.Read, FileShare.Read);
                                byte[] buffer = new byte[loc.Length];
                                int read = RandomAccess.Read(handle, buffer, loc.Offset);
                                if (read == loc.Length)
                                {
                                    chunkPayload = buffer;
                                }
                            }
                        }
                    }

                    var chunkResp = new ChunkPayloadResponse(chunkReq.ChunkHash, chunkPayload);
                    await SendResponseAsync(session, SyncMessageType.ChunkPayloadResponse, frame.CorrelationId, chunkResp, ct).ConfigureAwait(false);
                    break;

                case SyncMessageType.SyncCompletedNotice:
                    break;
            }
        }
        catch (Exception)
        {
            // Transient frame handling exception should not crash receiver
        }
    }

    private static bool IsResponse(SyncMessageType type) => type switch
    {
        SyncMessageType.MerkleRootResponse => true,
        SyncMessageType.PrefixDiffResponse => true,
        SyncMessageType.FileManifestResponse => true,
        SyncMessageType.ChunkPayloadResponse => true,
        _ => false
    };

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) == 0)
        {
            foreach (var kvp in _sessions)
            {
                kvp.Value.Dispose();
            }
            _sessions.Clear();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) == 0)
        {
            foreach (var kvp in _sessions)
            {
                await kvp.Value.DisposeAsync().ConfigureAwait(false);
            }
            _sessions.Clear();
        }
    }

    private sealed class ChannelSession : IAsyncDisposable, IDisposable
    {
        public IPeerTransportChannel Channel { get; }
        public ConcurrentDictionary<ulong, TaskCompletionSource<SyncWireFrame>> PendingRequests { get; } = new();
        public SemaphoreSlim SendLock { get; } = new(1, 1);
        public CancellationTokenSource Cts { get; } = new();
        public Task? ListenTask { get; private set; }

        private int _disposed;

        public ChannelSession(IPeerTransportChannel channel)
        {
            Channel = channel;
        }

        public void StartListening(SyncWireProtocol protocol)
        {
            ListenTask = Task.Run(() => ListenLoopAsync(protocol));
        }

        private async Task ListenLoopAsync(SyncWireProtocol protocol)
        {
            var ct = Cts.Token;
            try
            {
                while (!ct.IsCancellationRequested && Channel.IsConnected)
                {
                    ReadOnlyMemory<byte> frameBytes;
                    try
                    {
                        frameBytes = await Channel.ReceiveAsync(ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) { break; }
                    catch (Exception) { break; }

                    if (frameBytes.IsEmpty)
                    {
                        break;
                    }

                    SyncWireFrame frame;
                    try
                    {
                        frame = SyncWireFrameSerializer.Deserialize(frameBytes);
                    }
                    catch (Exception)
                    {
                        continue;
                    }

                    if (IsResponse(frame.Type))
                    {
                        if (PendingRequests.TryRemove(frame.CorrelationId, out var tcs))
                        {
                            tcs.TrySetResult(frame);
                        }
                    }
                    else
                    {
                        _ = protocol.HandleIncomingFrameAsync(this, frame, ct);
                    }
                }
            }
            finally
            {
                foreach (var kvp in PendingRequests)
                {
                    if (PendingRequests.TryRemove(kvp.Key, out var tcs))
                    {
                        tcs.TrySetException(new IOException($"Transport channel to {Channel.RemotePeerId} closed."));
                    }
                }
            }
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                Cts.Cancel();
                Cts.Dispose();
                SendLock.Dispose();
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                Cts.Cancel();
                if (ListenTask != null)
                {
                    try { await ListenTask.ConfigureAwait(false); } catch { }
                }
                Cts.Dispose();
                SendLock.Dispose();
            }
        }
    }

    private sealed class ChannelAttachment : IAsyncDisposable
    {
        private readonly SyncWireProtocol _protocol;
        private readonly ChannelSession _session;
        private int _detached;

        public ChannelAttachment(SyncWireProtocol protocol, ChannelSession session)
        {
            _protocol = protocol;
            _session = session;
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _detached, 1) == 0)
            {
                _protocol._sessions.TryRemove(_session.Channel, out _);
                await _session.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}
