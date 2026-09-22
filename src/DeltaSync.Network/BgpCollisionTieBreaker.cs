namespace DeltaSync.Network;

/// <summary>
/// Implements deterministic BGP-style connection collision resolution (RFC 4271 §6.8 &amp; Spec §3 Step 5).
/// When two peer nodes cross-dial simultaneously, lexicographical comparison of canonical PeerIds
/// deterministically determines which connection is preserved without distributed locks.
/// </summary>
public static class BgpCollisionTieBreaker
{
    /// <summary>
    /// Evaluates the RFC 4271 §6.8 collision rule between local node and remote peer.
    /// Returns <see cref="CollisionDecision.PreserveOutbound"/> if local PeerId is lexicographically greater;
    /// returns <see cref="CollisionDecision.YieldToInbound"/> if local PeerId is lexicographically smaller.
    /// Throws <see cref="InvalidOperationException"/> if both PeerIds are identical (self-connection loop).
    /// </summary>
    public static CollisionDecision Resolve(string localPeerId, string remotePeerId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localPeerId);
        ArgumentException.ThrowIfNullOrWhiteSpace(remotePeerId);

        int comparison = string.CompareOrdinal(localPeerId, remotePeerId);
        if (comparison == 0)
        {
            throw new InvalidOperationException($"Cannot resolve collision with identical PeerId '{localPeerId}' (self-connection loop).");
        }

        return comparison > 0
            ? CollisionDecision.PreserveOutbound
            : CollisionDecision.YieldToInbound;
    }

    /// <summary>
    /// Determines whether the local node should preserve its outbound dial when racing against an incoming dial.
    /// </summary>
    public static bool ShouldPreserveOutbound(string localPeerId, string remotePeerId) =>
        Resolve(localPeerId, remotePeerId) == CollisionDecision.PreserveOutbound;

    /// <summary>
    /// Determines whether the local node should yield its outbound dial and accept the incoming dial.
    /// </summary>
    public static bool ShouldYieldToInbound(string localPeerId, string remotePeerId) =>
        Resolve(localPeerId, remotePeerId) == CollisionDecision.YieldToInbound;
}
