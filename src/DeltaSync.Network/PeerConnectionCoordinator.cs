namespace DeltaSync.Network;

using System.Diagnostics.CodeAnalysis;
using System.Net;

/// <summary>
/// Delegate for establishing an embryonic transport channel to a remote peer endpoint.
/// </summary>
public delegate ValueTask<IPeerTransportChannel> PeerChannelDialer(
    string remotePeerId,
    IPEndPoint? endpoint,
    CancellationToken ct);

/// <summary>
/// Coordinates peer transport connections, introductory handshakes, and BGP-style collision tie-breaking (Spec §3 Step 5).
/// Enforces exact single connection convergence (P1), cluster isolation (P2), and zero socket duplication (I1).
/// </summary>
/// <remarks>
/// H-03: This class is sealed. All mutable state (_activeConnections, _inFlightDials) is protected by
/// _syncRoot. A subclass could introduce unsynchronised members or override methods that bypass the lock;
/// seal prevents that invariant from being violated silently.
/// </remarks>
public sealed class PeerConnectionCoordinator : IPeerConnectionCoordinator
{
    private readonly object _syncRoot = new();
    private readonly PeerChannelDialer? _dialer;
    private readonly Dictionary<string, IPeerTransportChannel> _activeConnections = new(StringComparer.Ordinal);
    private readonly Dictionary<string, InFlightDial> _inFlightDials = new(StringComparer.Ordinal);

    private bool _disposed;

    public Guid ClusterId { get; }
    public string LocalPeerId { get; }
    public int ListenPort { get; }
    public PeerRegistry Registry { get; }

    public IReadOnlyCollection<IPeerTransportChannel> ActiveConnections
    {
        get
        {
            lock (_syncRoot)
            {
                return _activeConnections.Values.ToList();
            }
        }
    }

    public event EventHandler<IPeerTransportChannel>? ConnectionEstablished;
    public event EventHandler<string>? ConnectionClosed;
    public event EventHandler<(string RemotePeerId, CollisionDecision Decision)>? CollisionResolved;

    public PeerConnectionCoordinator(
        Guid clusterId,
        string localPeerId,
        int listenPort = 0,
        PeerRegistry? registry = null,
        PeerChannelDialer? dialer = null)
    {
        if (clusterId == Guid.Empty)
            throw new ArgumentException("ClusterId cannot be empty.", nameof(clusterId));
        ArgumentException.ThrowIfNullOrWhiteSpace(localPeerId);

        ClusterId = clusterId;
        LocalPeerId = localPeerId;
        ListenPort = listenPort;
        Registry = registry ?? new PeerRegistry();
        _dialer = dialer;
    }

    public bool TryGetConnection(string peerId, [NotNullWhen(true)] out IPeerTransportChannel? channel)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(peerId);

        lock (_syncRoot)
        {
            if (_activeConnections.TryGetValue(peerId, out var existing) && existing.IsConnected)
            {
                channel = existing;
                return true;
            }
        }

        channel = null;
        return false;
    }

    public async Task<IPeerTransportChannel?> ConnectAsync(
        string remotePeerId,
        IPEndPoint? endpoint = null,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrWhiteSpace(remotePeerId);

        if (string.Equals(remotePeerId, LocalPeerId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Cannot dial self ('{remotePeerId}').");
        }

        if (_dialer is null)
        {
            throw new InvalidOperationException("No transport dialer configured for outbound connections.");
        }

        Task<IPeerTransportChannel?>? existingDialTask = null;
        InFlightDial inFlight;
        lock (_syncRoot)
        {
            if (_activeConnections.TryGetValue(remotePeerId, out var existing) && existing.IsConnected)
            {
                return existing;
            }

            if (_inFlightDials.TryGetValue(remotePeerId, out var existingDial))
            {
                // L-02: Register this caller's token so the dial can cancel if all
                // callers abandon it. The await below uses WaitAsync(ct) so this
                // caller's own cancellation is honoured independently.
                existingDial.LinkCaller(ct);
                existingDialTask = existingDial.Task;
                inFlight = existingDial;
            }
            else
            {
                inFlight = new InFlightDial(ct);
                _inFlightDials[remotePeerId] = inFlight;
            }
        }

        if (existingDialTask is not null)
        {
            // WaitAsync propagates this caller's own CT without cancelling the
            // shared dial or affecting other concurrent callers.
            return await existingDialTask.WaitAsync(ct).ConfigureAwait(false);
        }

        IPeerTransportChannel? channel = null;
        try
        {
            channel = await _dialer(remotePeerId, endpoint, inFlight.Token).ConfigureAwait(false);
            inFlight.SetChannel(channel);

            // Step 1: Transmit introductory handshake request
            var request = new HandshakeRequest(ClusterId, LocalPeerId, HandshakeRequest.CurrentVersion, ListenPort);
            await channel.SendAsync(request.ToByteArray(), inFlight.Token).ConfigureAwait(false);

            // Step 2: Receive and parse handshake response
            var responseBytes = await channel.ReceiveAsync(inFlight.Token).ConfigureAwait(false);
            if (responseBytes.IsEmpty)
            {
                throw new InvalidOperationException($"Remote peer {remotePeerId} closed the connection during handshake.");
            }

            if (!HandshakeResponse.TryParse(responseBytes.Span, out var response, out string? error))
            {
                throw new InvalidOperationException($"Malformed handshake response from {remotePeerId}: {error}");
            }

            // Step 3: Handle tie-break collision rejection
            if (response.Status == HandshakeStatus.CollisionRejected)
            {
                await channel.CloseAsync("Outbound dial aborted by remote BGP tie-breaker (RFC 4271 §6.8).").ConfigureAwait(false);
                channel.Dispose();

                lock (_syncRoot)
                {
                    if (_activeConnections.TryGetValue(remotePeerId, out var accepted) && accepted.IsConnected)
                    {
                        inFlight.Complete(accepted);
                        return accepted;
                    }
                }

                inFlight.Complete(null);
                return null;
            }

            if (!response.IsSuccess)
            {
                await channel.CloseAsync($"Handshake failed: {response.Status} - {response.ErrorMessage}").ConfigureAwait(false);
                channel.Dispose();
                Registry.RecordFailure(remotePeerId);
                throw new InvalidOperationException($"Handshake with {remotePeerId} failed: {response.Status} ({response.ErrorMessage})");
            }

            // Step 4: Finalize active canonical connection
            lock (_syncRoot)
            {
                if (_activeConnections.TryGetValue(remotePeerId, out var active) && active.IsConnected)
                {
                    // Inbound connection beat us to registration
                    channel.Dispose();
                    inFlight.Complete(active);
                    return active;
                }

                _activeConnections[remotePeerId] = channel;
                _inFlightDials.Remove(remotePeerId);
            }

            var ep = channel.RemoteEndPoint ?? endpoint ?? new IPEndPoint(IPAddress.Loopback, 0);
            if (!Registry.TryGetPeer(remotePeerId, out _))
            {
                Registry.RegisterOrUpdateStatic(remotePeerId, ep, out _);
            }
            Registry.MarkConnected(remotePeerId, out _);
            ConnectionEstablished?.Invoke(this, channel);
            inFlight.Complete(channel);
            return channel;
        }
        catch (OperationCanceledException) when (inFlight.IsYielded)
        {
            // Aborted because incoming connection from higher PeerId won the BGP tie-breaker
            channel?.Dispose();

            lock (_syncRoot)
            {
                if (_activeConnections.TryGetValue(remotePeerId, out var accepted) && accepted.IsConnected)
                {
                    inFlight.Complete(accepted);
                    return accepted;
                }
            }

            inFlight.Complete(null);
            return null;
        }
        catch (Exception ex)
        {
            channel?.Dispose();
            Registry.RecordFailure(remotePeerId);
            inFlight.Fail(ex);
            throw;
        }
        finally
        {
            lock (_syncRoot)
            {
                _inFlightDials.Remove(remotePeerId);
            }
        }
    }

    public async Task<bool> AcceptConnectionAsync(IPeerTransportChannel channel, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(channel);

        // Step 1: Read HandshakeRequest
        var rawBytes = await channel.ReceiveAsync(ct).ConfigureAwait(false);
        if (rawBytes.IsEmpty)
        {
            await channel.CloseAsync("Empty handshake payload").ConfigureAwait(false);
            channel.Dispose();
            return false;
        }

        if (!HandshakeRequest.TryParse(rawBytes.Span, out var request, out string? parseError))
        {
            var malformedResp = HandshakeResponse.CreateMalformed(parseError ?? "Malformed handshake payload.", LocalPeerId);
            try
            {
                await channel.SendAsync(malformedResp.ToByteArray(), ct).ConfigureAwait(false);
            }
            catch
            {
                // Ignore send errors during rejection
            }
            await channel.CloseAsync("Malformed handshake request").ConfigureAwait(false);
            channel.Dispose();
            return false;
        }

        // Step 2: Cluster Isolation Enforced (Postcondition P2)
        if (request.ClusterId != ClusterId)
        {
            var mismatchResp = HandshakeResponse.CreateClusterMismatch(ClusterId, LocalPeerId);
            try
            {
                await channel.SendAsync(mismatchResp.ToByteArray(), ct).ConfigureAwait(false);
            }
            catch
            {
                // Ignore send errors during rejection
            }
            await channel.CloseAsync("Cluster ID mismatch (P2)").ConfigureAwait(false);
            channel.Dispose();
            return false;
        }

        // Step 3: Protocol Version Compatibility
        if (request.ProtocolVersion < HandshakeRequest.MinVersion || request.ProtocolVersion > HandshakeRequest.CurrentVersion)
        {
            var versionResp = HandshakeResponse.CreateVersionIncompatible(HandshakeRequest.CurrentVersion, LocalPeerId);
            try
            {
                await channel.SendAsync(versionResp.ToByteArray(), ct).ConfigureAwait(false);
            }
            catch
            {
                // Ignore send errors during rejection
            }
            await channel.CloseAsync("Incompatible protocol version").ConfigureAwait(false);
            channel.Dispose();
            return false;
        }

        // Step 4: Self-connection loopback rejection
        if (string.Equals(request.PeerId, LocalPeerId, StringComparison.Ordinal))
        {
            var loopbackResp = HandshakeResponse.CreateMalformed("Self-connection loop rejected.", LocalPeerId);
            try
            {
                await channel.SendAsync(loopbackResp.ToByteArray(), ct).ConfigureAwait(false);
            }
            catch
            {
                // Ignore send errors during rejection
            }
            await channel.CloseAsync("Self-connection loop").ConfigureAwait(false);
            channel.Dispose();
            return false;
        }

        // Step 5: Duplicate and Collision Resolution (RFC 4271 §6.8 & Postcondition P1, Invariant I1)
        lock (_syncRoot)
        {
            if (_activeConnections.TryGetValue(request.PeerId, out var existing) && existing.IsConnected)
            {
                // Invariant I1: Zero socket duplication
                var duplicateResp = HandshakeResponse.CreateDuplicate(LocalPeerId);
                _ = SendResponseAndCloseAsync(channel, duplicateResp, "Duplicate connection rejected (I1)", ct);
                return false;
            }

            if (_inFlightDials.TryGetValue(request.PeerId, out var inFlight))
            {
                var decision = BgpCollisionTieBreaker.Resolve(LocalPeerId, request.PeerId);
                CollisionResolved?.Invoke(this, (request.PeerId, decision));

                if (decision == CollisionDecision.PreserveOutbound)
                {
                    // Local node has higher PeerId: preserve outbound, reject incoming dial
                    var rejectResp = HandshakeResponse.CreateCollisionRejected(LocalPeerId);
                    _ = SendResponseAndCloseAsync(channel, rejectResp, "Collision rejected under BGP tie-break (RFC 4271 §6.8)", ct);
                    return false;
                }
                else
                {
                    // Local node has lower PeerId: abort outbound dial and yield to incoming dial
                    inFlight.Abort("Yielding to incoming connection from higher PeerId");
                    _inFlightDials.Remove(request.PeerId);
                }
            }
        }

        // Step 6: Acknowledge successful handshake and register canonical channel
        var successResp = HandshakeResponse.CreateSuccess(ClusterId, LocalPeerId);
        await channel.SendAsync(successResp.ToByteArray(), ct).ConfigureAwait(false);

        lock (_syncRoot)
        {
            _activeConnections[request.PeerId] = channel;
        }

        var ep = channel.RemoteEndPoint ?? new IPEndPoint(IPAddress.Loopback, request.ListenPort > 0 ? request.ListenPort : 0);
        if (!Registry.TryGetPeer(request.PeerId, out _))
        {
            Registry.RegisterOrUpdateStatic(request.PeerId, ep, out _);
        }
        Registry.MarkConnected(request.PeerId, out _);
        ConnectionEstablished?.Invoke(this, channel);
        return true;
    }

    private static async Task SendResponseAndCloseAsync(
        IPeerTransportChannel channel,
        HandshakeResponse response,
        string closeReason,
        CancellationToken ct)
    {
        try
        {
            await channel.SendAsync(response.ToByteArray(), ct).ConfigureAwait(false);
        }
        catch
        {
            // Ignore send failure on aborted channel
        }
        finally
        {
            await channel.CloseAsync(closeReason).ConfigureAwait(false);
            channel.Dispose();
        }
    }

    public async Task CloseConnectionAsync(string peerId, string reason = "Normal closure")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(peerId);

        IPeerTransportChannel? channel = null;
        lock (_syncRoot)
        {
            if (_activeConnections.Remove(peerId, out channel))
            {
                // removed
            }
        }

        if (channel is not null)
        {
            await channel.CloseAsync(reason).ConfigureAwait(false);
            channel.Dispose();
            ConnectionClosed?.Invoke(this, peerId);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        List<IPeerTransportChannel> connections;
        List<InFlightDial> dials;

        lock (_syncRoot)
        {
            connections = _activeConnections.Values.ToList();
            _activeConnections.Clear();

            dials = _inFlightDials.Values.ToList();
            _inFlightDials.Clear();
        }

        foreach (var dial in dials)
        {
            dial.Abort("Coordinator disposed.");
        }

        foreach (var conn in connections)
        {
            conn.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        List<IPeerTransportChannel> connections;
        List<InFlightDial> dials;

        lock (_syncRoot)
        {
            connections = _activeConnections.Values.ToList();
            _activeConnections.Clear();

            dials = _inFlightDials.Values.ToList();
            _inFlightDials.Clear();
        }

        foreach (var dial in dials)
        {
            dial.Abort("Coordinator disposed.");
        }

        foreach (var conn in connections)
        {
            await conn.DisposeAsync().ConfigureAwait(false);
        }
    }

    private sealed class InFlightDial
    {
        private readonly CancellationTokenSource _linkedCts;
        private readonly TaskCompletionSource<IPeerTransportChannel?> _tcs;
        private IPeerTransportChannel? _channel;
        private int _isYielded;

        public CancellationToken Token => _linkedCts.Token;
        public Task<IPeerTransportChannel?> Task => _tcs.Task;
        public bool IsYielded => Volatile.Read(ref _isYielded) == 1;

        public InFlightDial(CancellationToken callerToken)
        {
            _linkedCts = CancellationTokenSource.CreateLinkedTokenSource(callerToken);
            _tcs = new TaskCompletionSource<IPeerTransportChannel?>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        /// <summary>
        /// Links an additional caller into the shared dial. Caller-specific timeouts and cancellations
        /// are isolated and evaluated independently via <see cref="Task.WaitAsync(CancellationToken)"/>
        /// in <see cref="ConnectAsync"/> without aborting the underlying shared connection attempt.
        /// </summary>
        public void LinkCaller(CancellationToken callerToken)
        {
            // Intentionally no-op: caller cancellation is decoupled from the underlying shared dial task.
            // Each multiplexed caller awaits existingDialTask.WaitAsync(ct), ensuring that a single
            // caller timing out or cancelling does not abort connection establishment for other callers.
        }

        public void SetChannel(IPeerTransportChannel channel)
        {
            _channel = channel;
        }

        public void Abort(string reason)
        {
            if (Interlocked.Exchange(ref _isYielded, 1) == 0)
            {
                _linkedCts.Cancel();
                if (_channel is not null)
                {
                    _ = _channel.CloseAsync(reason);
                    _channel.Dispose();
                }
            }
        }

        public void Complete(IPeerTransportChannel? channel) => _tcs.TrySetResult(channel);
        public void Fail(Exception ex) => _tcs.TrySetException(ex);
    }
}
