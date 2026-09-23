namespace DeltaSync.Network;

using System.Net;

/// <summary>
/// Coordinates peer transport connections, introductory handshakes, and BGP-style collision tie-breaking (Spec §3 Step 5).
/// Enforces exact single connection convergence (P1), cluster isolation (P2), and zero socket duplication (I1).
/// </summary>
public interface IPeerConnectionCoordinator : IPeerChannelProvider, IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Local synchronization cluster UUID.
    /// </summary>
    Guid ClusterId { get; }

    /// <summary>
    /// Canonical local peer identifier.
    /// </summary>
    string LocalPeerId { get; }

    /// <summary>
    /// Local TCP/gRPC server port.
    /// </summary>
    int ListenPort { get; }

    /// <summary>
    /// Peer liveness and metadata registry.
    /// </summary>
    PeerRegistry Registry { get; }

    /// <summary>
    /// Event raised when a cross-dial collision is detected and resolved under RFC 4271 §6.8 tie-break.
    /// </summary>
    event EventHandler<(string RemotePeerId, CollisionDecision Decision)>? CollisionResolved;

    /// <summary>
    /// Initiates an outbound transport dial to a remote peer with handshake negotiation.
    /// Returns the canonical channel, or null if this dial yielded under BGP collision tie-break.
    /// </summary>
    Task<IPeerTransportChannel?> ConnectAsync(string remotePeerId, IPEndPoint? endpoint = null, CancellationToken ct = default);

    /// <summary>
    /// Processes an incoming transport channel initiated by a remote peer, negotiating the introductory handshake.
    /// Returns true if the channel was accepted; false if rejected (cluster mismatch, duplicate, or collision).
    /// </summary>
    Task<bool> AcceptConnectionAsync(IPeerTransportChannel channel, CancellationToken ct = default);

    /// <summary>
    /// Forcibly closes and disconnects an active peer channel.
    /// </summary>
    Task CloseConnectionAsync(string peerId, string reason = "Normal closure");
}
