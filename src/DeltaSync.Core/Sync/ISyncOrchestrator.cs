using DeltaSync.Network;

namespace DeltaSync.Core.Sync;

/// <summary>
/// Core port interface for the end-to-end synchronization orchestrator.
/// Coordinates local file watcher ingestion, peer wire negotiation, causality conflict resolution,
/// crash-safe temporary staging, and atomic commits. Spec §2.2, §3 Steps 3–4.
/// </summary>
public interface ISyncOrchestrator : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Gets the root directory monitored and synchronized by this orchestrator.
    /// </summary>
    string SyncRootDirectory { get; }

    /// <summary>
    /// Gets the local peer identifier.
    /// </summary>
    string LocalPeerId { get; }

    /// <summary>
    /// Gets a value indicating whether the orchestrator is actively running.
    /// </summary>
    bool IsRunning { get; }

    /// <summary>
    /// Starts the orchestrator, performing crash recovery cleanup and beginning file watching and peer synchronization.
    /// </summary>
    Task StartAsync(CancellationToken ct = default);

    /// <summary>
    /// Stops the orchestrator and pauses synchronization and watching.
    /// </summary>
    Task StopAsync(CancellationToken ct = default);

    /// <summary>
    /// Executes a full anti-entropy synchronization cycle against a specific peer transport channel.
    /// Returns true if changes were synchronized; false if replicas were already identical.
    /// </summary>
    Task<bool> SynchronizeAsync(IPeerTransportChannel channel, CancellationToken ct = default);

    /// <summary>
    /// Executes synchronization against all currently active peer channels.
    /// </summary>
    Task SynchronizeAllAsync(CancellationToken ct = default);

    /// <summary>
    /// Scans the temporary staging directory for abandoned/orphaned .tmp files from previous crashed sessions
    /// and deletes them. Returns the number of files cleaned up.
    /// </summary>
    int CleanupStagingFiles();
}
