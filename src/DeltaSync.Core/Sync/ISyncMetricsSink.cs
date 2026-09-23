namespace DeltaSync.Core.Sync;

/// <summary>
/// Observability port for capturing synchronization telemetry and audit metrics (Spec §2.2).
/// </summary>
public interface ISyncMetricsSink
{
    /// <summary>
    /// Records payload bytes transferred over the network transport channel.
    /// </summary>
    void RecordBytesTransferred(long bytes, bool isOutgoing);

    /// <summary>
    /// Records payload bytes saved via FastCDC chunk deduplication.
    /// </summary>
    void RecordChunkDeduplicated(long bytes);

    /// <summary>
    /// Records a concurrent offline edit conflict detection event (ADR-0001).
    /// </summary>
    void RecordConflictDetected();

    /// <summary>
    /// Records the wall-clock execution duration of a sync cycle.
    /// </summary>
    void RecordSyncCycleCompleted(TimeSpan duration);
}

/// <summary>
/// No-op implementation of ISyncMetricsSink for testing and default configurations.
/// </summary>
public sealed class NullSyncMetricsSink : ISyncMetricsSink
{
    public static readonly NullSyncMetricsSink Instance = new();

    public void RecordBytesTransferred(long bytes, bool isOutgoing) { }
    public void RecordChunkDeduplicated(long bytes) { }
    public void RecordConflictDetected() { }
    public void RecordSyncCycleCompleted(TimeSpan duration) { }
}
