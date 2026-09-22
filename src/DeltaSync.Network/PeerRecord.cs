namespace DeltaSync.Network;

using System.Net;

/// <summary>
/// Represents an in-memory tracking record for a remote DeltaSync peer.
/// </summary>
public sealed class PeerRecord
{
    private readonly object _syncRoot = new();

    public string PeerId { get; internal set; }
    public byte[]? PeerIdHash { get; internal set; }
    public IPEndPoint Endpoint { get; internal set; }
    public PeerState State { get; internal set; }
    public DateTimeOffset LastSeen { get; internal set; }
    public DateTimeOffset? LastConnected { get; internal set; }
    public ulong LastSequence { get; internal set; }
    public bool IsStatic { get; init; }
    public int ConsecutiveFailures { get; internal set; }

    public PeerRecord(
        string peerId,
        IPEndPoint endpoint,
        PeerState state,
        DateTimeOffset lastSeen,
        byte[]? peerIdHash = null,
        ulong sequence = 0,
        bool isStatic = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(peerId);
        ArgumentNullException.ThrowIfNull(endpoint);

        PeerId = peerId;
        Endpoint = endpoint;
        State = state;
        LastSeen = lastSeen;
        PeerIdHash = peerIdHash;
        LastSequence = sequence;
        IsStatic = isStatic;
        ConsecutiveFailures = 0;
    }

    /// <summary>
    /// Computes the elapsed time since the peer's last seen beacon or activity.
    /// </summary>
    public TimeSpan GetElapsedSinceLastSeen(TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        return timeProvider.GetUtcNow() - LastSeen;
    }

    /// <summary>
    /// Creates a snapshot copy of this peer record.
    /// </summary>
    public PeerRecord Snapshot()
    {
        lock (_syncRoot)
        {
            return new PeerRecord(
                PeerId,
                Endpoint,
                State,
                LastSeen,
                PeerIdHash is null ? null : (byte[])PeerIdHash.Clone(),
                LastSequence,
                IsStatic)
            {
                LastConnected = LastConnected,
                ConsecutiveFailures = ConsecutiveFailures
            };
        }
    }

    public override string ToString() =>
        $"Peer[{PeerId} @ {Endpoint}, State={State}, LastSeen={LastSeen:HH:mm:ss.fff}, Static={IsStatic}]";
}
