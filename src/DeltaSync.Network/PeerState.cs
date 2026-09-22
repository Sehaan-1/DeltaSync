namespace DeltaSync.Network;

/// <summary>
/// Represents the dynamic lifecycle state of a peer node in DeltaSync (Spec §3 Step 4).
/// </summary>
public enum PeerState
{
    /// <summary>
    /// Peer beacon heard for the first time or static endpoint configured.
    /// Awaiting or attempting transport connection.
    /// </summary>
    Discovered,

    /// <summary>
    /// Bidirectional transport channel established and handshake acknowledged.
    /// </summary>
    Connected,

    /// <summary>
    /// No beacon received for > 6.0 seconds (2 missed intervals), but transport channel may still be active.
    /// </summary>
    Stale,

    /// <summary>
    /// No beacon received for > 9.0 seconds and transport disconnected.
    /// Pruned from active peers; emits PeerLost event.
    /// </summary>
    Dead
}
