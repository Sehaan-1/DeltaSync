namespace DeltaSync.Network;

/// <summary>
/// Port abstraction for peer connection lifecycle events and active channel resolution.
/// Owned by DeltaSync.Core as an external port (docs/architecture/deltasync.md § Ports the Core Owns).
/// </summary>
public interface IPeerChannelProvider
{
    /// <summary>
    /// Event raised when a canonical duplex connection is successfully established.
    /// </summary>
    event EventHandler<IPeerTransportChannel>? ConnectionEstablished;

    /// <summary>
    /// Event raised when an active peer connection is terminated or closed.
    /// </summary>
    event EventHandler<string>? ConnectionClosed;

    /// <summary>
    /// Collection of currently active transport channels.
    /// </summary>
    IReadOnlyCollection<IPeerTransportChannel> ActiveConnections { get; }

    /// <summary>
    /// Attempts to retrieve an active transport channel by canonical peer ID.
    /// </summary>
    bool TryGetConnection(string peerId, out IPeerTransportChannel? channel);
}
