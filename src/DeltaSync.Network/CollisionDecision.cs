namespace DeltaSync.Network;

/// <summary>
/// Decision outcome of the RFC 4271 §6.8 BGP-style connection collision tie-breaker.
/// </summary>
public enum CollisionDecision
{
    /// <summary>
    /// Local node has higher PeerId: preserve outbound connection and reject/abort incoming connection attempt.
    /// </summary>
    PreserveOutbound,

    /// <summary>
    /// Local node has lower PeerId: abort outbound connection attempt and accept incoming connection.
    /// </summary>
    YieldToInbound
}
