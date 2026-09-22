namespace DeltaSync.Network;

using System.Diagnostics.CodeAnalysis;
using System.Net;

/// <summary>
/// Thread-safe in-memory registry of discovered and static peer nodes (Spec §3 Step 4).
/// Indexes peers by canonical PeerId, SHA-256 PeerIdHash, and service IPEndPoint.
/// </summary>
public sealed class PeerRegistry
{
    private readonly object _syncRoot = new();
    private readonly TimeProvider _timeProvider;

    private readonly Dictionary<string, PeerRecord> _peersById = new(StringComparer.Ordinal);
    private readonly Dictionary<string, PeerRecord> _peersByHashHex = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<IPEndPoint, PeerRecord> _peersByEndpoint = new();

    /// <summary>
    /// Event raised when a new peer is discovered via beacon or static registration.
    /// </summary>
    public event EventHandler<PeerRecord>? PeerDiscovered;

    /// <summary>
    /// Event raised when a peer transitions to Connected.
    /// </summary>
    public event EventHandler<PeerRecord>? PeerConnected;

    /// <summary>
    /// Event raised when a peer transitions to Stale (missed beacons for > 6.0s).
    /// </summary>
    public event EventHandler<PeerRecord>? PeerStale;

    /// <summary>
    /// Event raised when a peer transitions to Dead / evicted (missed beacons for > 9.0s).
    /// </summary>
    public event EventHandler<PeerRecord>? PeerLost;

    public PeerRegistry(TimeProvider? timeProvider = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>
    /// Number of peers currently tracked in any state.
    /// </summary>
    public int TotalCount
    {
        get
        {
            lock (_syncRoot)
            {
                return _peersById.Count;
            }
        }
    }

    /// <summary>
    /// Number of active peers (state is Discovered, Connected, or Stale).
    /// </summary>
    public int ActiveCount
    {
        get
        {
            lock (_syncRoot)
            {
                return _peersById.Values.Count(p => p.State != PeerState.Dead);
            }
        }
    }

    /// <summary>
    /// Registers a newly heard beacon or updates an existing peer's last seen timestamp and endpoint.
    /// Returns true if this was a new discovery; false if it refreshed an existing peer.
    /// </summary>
    public bool RegisterOrUpdateBeacon(DiscoveredBeacon beacon, string? knownPeerId, out PeerRecord peer)
    {
        ArgumentNullException.ThrowIfNull(beacon);

        string hashHex = Convert.ToHexString(beacon.Frame.PeerIdHash);
        var serviceEndPoint = beacon.ServiceEndPoint;
        var now = _timeProvider.GetUtcNow();

        PeerRecord? discoveredNew = null;
        PeerRecord? stateRestored = null;

        lock (_syncRoot)
        {
            // 1. Check if peer exists by hash or endpoint
            if (_peersByHashHex.TryGetValue(hashHex, out var existing) ||
                _peersByEndpoint.TryGetValue(serviceEndPoint, out existing))
            {
                existing.LastSeen = now;
                existing.Endpoint = serviceEndPoint;

                if (beacon.Frame.Sequence > existing.LastSequence)
                {
                    existing.LastSequence = beacon.Frame.Sequence;
                }

                // If peer was previously Stale, restore to Discovered
                if (existing.State == PeerState.Stale)
                {
                    existing.State = PeerState.Discovered;
                    stateRestored = existing.Snapshot();
                }

                // Associate known canonical PeerId if supplied and not set
                if (!string.IsNullOrEmpty(knownPeerId) &&
                    !string.Equals(existing.PeerId, knownPeerId, StringComparison.Ordinal))
                {
                    _peersById.Remove(existing.PeerId);
                    existing.PeerId = knownPeerId;
                    _peersById[knownPeerId] = existing;
                }

                // Ensure hash index is updated
                _peersByHashHex[hashHex] = existing;
                _peersByEndpoint[serviceEndPoint] = existing;

                peer = existing.Snapshot();
            }
            else
            {
                // New discovery
                string peerId = !string.IsNullOrEmpty(knownPeerId)
                    ? knownPeerId
                    : hashHex[..16].ToLowerInvariant();

                var newRecord = new PeerRecord(
                    peerId,
                    serviceEndPoint,
                    PeerState.Discovered,
                    now,
                    beacon.Frame.PeerIdHash,
                    beacon.Frame.Sequence,
                    isStatic: false);

                _peersById[peerId] = newRecord;
                _peersByHashHex[hashHex] = newRecord;
                _peersByEndpoint[serviceEndPoint] = newRecord;

                peer = newRecord.Snapshot();
                discoveredNew = peer;
            }
        }

        if (discoveredNew is not null)
        {
            PeerDiscovered?.Invoke(this, discoveredNew);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Registers or updates a static peer address (configured via --peer).
    /// </summary>
    public bool RegisterOrUpdateStatic(string peerIdOrEndpoint, IPEndPoint endpoint, out PeerRecord peer)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(peerIdOrEndpoint);
        ArgumentNullException.ThrowIfNull(endpoint);

        var now = _timeProvider.GetUtcNow();
        PeerRecord? discoveredNew = null;

        lock (_syncRoot)
        {
            if (_peersById.TryGetValue(peerIdOrEndpoint, out var existing) ||
                _peersByEndpoint.TryGetValue(endpoint, out existing))
            {
                existing.LastSeen = now;
                existing.Endpoint = endpoint;
                peer = existing.Snapshot();
            }
            else
            {
                var newRecord = new PeerRecord(
                    peerIdOrEndpoint,
                    endpoint,
                    PeerState.Discovered,
                    now,
                    peerIdHash: null,
                    sequence: 0,
                    isStatic: true);

                _peersById[peerIdOrEndpoint] = newRecord;
                _peersByEndpoint[endpoint] = newRecord;

                peer = newRecord.Snapshot();
                discoveredNew = peer;
            }
        }

        if (discoveredNew is not null)
        {
            PeerDiscovered?.Invoke(this, discoveredNew);
            return true;
        }

        return false;
    }

    public bool TryGetPeer(string peerId, [NotNullWhen(true)] out PeerRecord? peer)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(peerId);

        lock (_syncRoot)
        {
            if (_peersById.TryGetValue(peerId, out var existing))
            {
                peer = existing.Snapshot();
                return true;
            }
        }

        peer = null;
        return false;
    }

    public bool TryGetPeerByHash(ReadOnlySpan<byte> peerIdHash, [NotNullWhen(true)] out PeerRecord? peer)
    {
        string hashHex = Convert.ToHexString(peerIdHash);

        lock (_syncRoot)
        {
            if (_peersByHashHex.TryGetValue(hashHex, out var existing))
            {
                peer = existing.Snapshot();
                return true;
            }
        }

        peer = null;
        return false;
    }

    public bool TryGetPeerByEndpoint(IPEndPoint endpoint, [NotNullWhen(true)] out PeerRecord? peer)
    {
        ArgumentNullException.ThrowIfNull(endpoint);

        lock (_syncRoot)
        {
            if (_peersByEndpoint.TryGetValue(endpoint, out var existing))
            {
                peer = existing.Snapshot();
                return true;
            }
        }

        peer = null;
        return false;
    }

    /// <summary>
    /// Transitions a peer to Connected state, resetting failed attempts and updating LastConnected timestamp.
    /// </summary>
    public bool MarkConnected(string peerId, [NotNullWhen(true)] out PeerRecord? peer)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(peerId);

        PeerRecord? snapshot = null;
        lock (_syncRoot)
        {
            if (_peersById.TryGetValue(peerId, out var existing))
            {
                var now = _timeProvider.GetUtcNow();
                existing.State = PeerState.Connected;
                existing.LastSeen = now;
                existing.LastConnected = now;
                existing.ConsecutiveFailures = 0;
                snapshot = existing.Snapshot();
                peer = snapshot;
            }
            else
            {
                peer = null;
                return false;
            }
        }

        PeerConnected?.Invoke(this, snapshot);
        return true;
    }

    /// <summary>
    /// Transitions a peer to Stale state (missed beacons for > 6.0s).
    /// </summary>
    public bool MarkStale(string peerId, [NotNullWhen(true)] out PeerRecord? peer)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(peerId);

        PeerRecord? snapshot = null;
        lock (_syncRoot)
        {
            if (_peersById.TryGetValue(peerId, out var existing))
            {
                existing.State = PeerState.Stale;
                snapshot = existing.Snapshot();
                peer = snapshot;
            }
            else
            {
                peer = null;
                return false;
            }
        }

        PeerStale?.Invoke(this, snapshot);
        return true;
    }

    /// <summary>
    /// Transitions a peer to Dead state (missed beacons for > 9.0s), optionally evicting from active tracking.
    /// Emits PeerLost event.
    /// </summary>
    public bool MarkDead(string peerId, bool evict, [NotNullWhen(true)] out PeerRecord? peer)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(peerId);

        PeerRecord? snapshot = null;
        lock (_syncRoot)
        {
            if (_peersById.TryGetValue(peerId, out var existing))
            {
                existing.State = PeerState.Dead;
                snapshot = existing.Snapshot();

                if (evict)
                {
                    _peersById.Remove(peerId);
                    if (existing.PeerIdHash is not null)
                    {
                        _peersByHashHex.Remove(Convert.ToHexString(existing.PeerIdHash));
                    }
                    _peersByEndpoint.Remove(existing.Endpoint);
                }

                peer = snapshot;
            }
            else
            {
                peer = null;
                return false;
            }
        }

        PeerLost?.Invoke(this, snapshot);
        return true;
    }

    /// <summary>
    /// Increments consecutive connection failure count for backoff tracking.
    /// </summary>
    public int RecordFailure(string peerId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(peerId);

        lock (_syncRoot)
        {
            if (_peersById.TryGetValue(peerId, out var existing))
            {
                existing.ConsecutiveFailures++;
                return existing.ConsecutiveFailures;
            }
        }

        return 0;
    }

    /// <summary>
    /// Resets consecutive connection failure count.
    /// </summary>
    public void ResetFailures(string peerId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(peerId);

        lock (_syncRoot)
        {
            if (_peersById.TryGetValue(peerId, out var existing))
            {
                existing.ConsecutiveFailures = 0;
            }
        }
    }

    public IReadOnlyList<PeerRecord> GetAllPeers()
    {
        lock (_syncRoot)
        {
            return _peersById.Values.Select(p => p.Snapshot()).ToList();
        }
    }

    public IReadOnlyList<PeerRecord> GetActivePeers()
    {
        lock (_syncRoot)
        {
            return _peersById.Values
                .Where(p => p.State != PeerState.Dead)
                .Select(p => p.Snapshot())
                .ToList();
        }
    }

    public void Clear()
    {
        lock (_syncRoot)
        {
            _peersById.Clear();
            _peersByHashHex.Clear();
            _peersByEndpoint.Clear();
        }
    }
}
