namespace DeltaSync.Network;

/// <summary>
/// Result summary of a synchronous or periodic peer liveness sweep.
/// </summary>
public sealed record LivenessSweepResult(int PeersChecked, int TransitionedToStale, int TransitionedToDead);

/// <summary>
/// Manages peer heartbeat timeout evaluations using a monotonic TimeProvider (Spec §3 Step 4 & Postcondition P3).
/// Moves peers to Stale (> 6.0s silence) and Dead (> 9.0s silence) and evicts dead nodes.
/// </summary>
public sealed class PeerLivenessTracker : IAsyncDisposable, IDisposable
{
    private readonly PeerRegistry _registry;
    private readonly PeerLivenessOptions _options;
    private readonly TimeProvider _timeProvider;

    private ITimer? _timer;
    private int _isSweeping;
    private bool _disposed;

    public PeerLivenessTracker(
        PeerRegistry registry,
        PeerLivenessOptions? options = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(registry);

        _registry = registry;
        _options = options ?? new PeerLivenessOptions();
        _options.Validate();
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>
    /// Starts periodic background liveness sweeps.
    /// </summary>
    public void Start()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_timer is not null) return;

        _timer = _timeProvider.CreateTimer(
            callback: _ => EvaluateLiveness(),
            state: null,
            dueTime: _options.SweepInterval,
            period: _options.SweepInterval);
    }

    /// <summary>
    /// Synchronously executes a liveness evaluation across all tracked peers against the current TimeProvider clock.
    /// </summary>
    public LivenessSweepResult EvaluateLiveness()
    {
        if (Interlocked.CompareExchange(ref _isSweeping, 1, 0) != 0)
        {
            // Another sweep is already in progress
            return new LivenessSweepResult(0, 0, 0);
        }

        try
        {
            var now = _timeProvider.GetUtcNow();
            var peers = _registry.GetAllPeers();

            int checkedCount = 0;
            int staleCount = 0;
            int deadCount = 0;

            foreach (var peer in peers)
            {
                checkedCount++;
                var elapsed = now - peer.LastSeen;

                switch (peer.State)
                {
                    case PeerState.Discovered:
                    case PeerState.Connected:
                        if (elapsed >= _options.DeadTimeout)
                        {
                            if (_registry.MarkDead(peer.PeerId, _options.EvictOnDead, out _))
                            {
                                deadCount++;
                            }
                        }
                        else if (elapsed >= _options.StaleTimeout)
                        {
                            if (_registry.MarkStale(peer.PeerId, out _))
                            {
                                staleCount++;
                            }
                        }
                        break;

                    case PeerState.Stale:
                        if (elapsed >= _options.DeadTimeout)
                        {
                            if (_registry.MarkDead(peer.PeerId, _options.EvictOnDead, out _))
                            {
                                deadCount++;
                            }
                        }
                        break;

                    case PeerState.Dead:
                        // Already dead; no further action
                        break;
                }
            }

            return new LivenessSweepResult(checkedCount, staleCount, deadCount);
        }
        finally
        {
            Interlocked.Exchange(ref _isSweeping, 0);
        }
    }

    public Task StopAsync()
    {
        Dispose();
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _timer?.Dispose();
        _timer = null;
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
