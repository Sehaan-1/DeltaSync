namespace DeltaSync.Network;

/// <summary>
/// Configuration parameters for peer heartbeat timeouts and periodic liveness evaluation (Spec §3 Step 4 & §6).
/// </summary>
public sealed record PeerLivenessOptions
{
    /// <summary>
    /// Default heartbeat stale duration: 6.0 seconds (2 missed 3.0s beacon intervals).
    /// </summary>
    public static readonly TimeSpan DefaultStaleTimeout = TimeSpan.FromSeconds(6.0);

    /// <summary>
    /// Default peer heartbeat timeout: 9.0 seconds (3 missed 3.0s beacon intervals).
    /// </summary>
    public static readonly TimeSpan DefaultDeadTimeout = TimeSpan.FromSeconds(9.0);

    /// <summary>
    /// Default periodic sweep interval: 1.0 second (Postcondition P3 [9.0s, 10.0s]).
    /// </summary>
    public static readonly TimeSpan DefaultSweepInterval = TimeSpan.FromSeconds(1.0);

    /// <summary>
    /// Duration of silence before a peer is marked Stale.
    /// </summary>
    public TimeSpan StaleTimeout { get; init; } = DefaultStaleTimeout;

    /// <summary>
    /// Duration of silence before a peer is marked Dead and pruned from active peers.
    /// </summary>
    public TimeSpan DeadTimeout { get; init; } = DefaultDeadTimeout;

    /// <summary>
    /// Period between background sweeps.
    /// </summary>
    public TimeSpan SweepInterval { get; init; } = DefaultSweepInterval;

    /// <summary>
    /// Whether dead peers are evicted from the active registry dictionary upon transition.
    /// </summary>
    public bool EvictOnDead { get; init; } = true;

    public void Validate()
    {
        if (StaleTimeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(StaleTimeout), "StaleTimeout must be positive.");
        if (DeadTimeout <= StaleTimeout)
            throw new ArgumentOutOfRangeException(nameof(DeadTimeout), "DeadTimeout must be strictly greater than StaleTimeout.");
        if (SweepInterval <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(SweepInterval), "SweepInterval must be positive.");
    }
}
