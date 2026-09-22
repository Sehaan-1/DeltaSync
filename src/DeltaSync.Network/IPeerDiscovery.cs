namespace DeltaSync.Network;

/// <summary>
/// Unifies local broadcast discovery beacons and configured static endpoints into a single peer event stream (ADR-0002).
/// </summary>
public interface IPeerDiscovery : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Event raised when a peer is discovered via beacon broadcast or static registration.
    /// </summary>
    event EventHandler<PeerRecord>? PeerDiscovered;

    /// <summary>
    /// Event raised when a peer is marked connected.
    /// </summary>
    event EventHandler<PeerRecord>? PeerConnected;

    /// <summary>
    /// Event raised when a peer misses beacons for > 6.0 seconds.
    /// </summary>
    event EventHandler<PeerRecord>? PeerStale;

    /// <summary>
    /// Event raised when a peer misses beacons for > 9.0 seconds and is evicted.
    /// </summary>
    event EventHandler<PeerRecord>? PeerLost;

    /// <summary>
    /// Collection of currently active peers (Discovered, Connected, or Stale).
    /// </summary>
    IReadOnlyCollection<PeerRecord> ActivePeers { get; }

    /// <summary>
    /// Underlying peer registry.
    /// </summary>
    PeerRegistry Registry { get; }

    /// <summary>
    /// Starts local beacon announcement, multicast listening, and liveness monitoring.
    /// </summary>
    Task StartAsync(CancellationToken ct = default);

    /// <summary>
    /// Stops all background networking and timers.
    /// </summary>
    Task StopAsync();
}
