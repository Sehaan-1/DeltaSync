namespace DeltaSync.Network;

/// <summary>
/// Result status codes for peer introductory handshake negotiation (Spec §3 Step 5).
/// </summary>
public enum HandshakeStatus
{
    /// <summary>
    /// Handshake completed successfully. Duplex channel is authorized for synchronization.
    /// </summary>
    Success = 0,

    /// <summary>
    /// Cluster UUID mismatch. Peers are in different folder clusters and must remain isolated (Postcondition P2).
    /// </summary>
    ClusterMismatch = 1,

    /// <summary>
    /// Incompatible protocol version between connecting peers.
    /// </summary>
    VersionIncompatible = 2,

    /// <summary>
    /// Connection collision resolved under RFC 4271 §6.8 tie-breaker; this connection yields to the surviving dial.
    /// </summary>
    CollisionRejected = 3,

    /// <summary>
    /// Duplicate connection rejected because a healthy channel is already active for this PeerId (Invariant I1).
    /// </summary>
    DuplicateConnection = 4,

    /// <summary>
    /// Handshake request payload was malformed, missing required fields, or exceeded size limits.
    /// </summary>
    MalformedRequest = 5
}
