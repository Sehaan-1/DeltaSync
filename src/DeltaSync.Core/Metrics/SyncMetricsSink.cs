using System.Globalization;
using DeltaSync.Core.Sync;

namespace DeltaSync.Core.Metrics;

/// <summary>
/// Thread-safe in-memory sink for recording sync performance telemetry and counters (ADR-0005, Spec §2.2).
/// </summary>
public sealed class SyncMetricsSink : ISyncMetricsSink
{
    private long _inboundBytesTransferred;
    private long _outboundBytesTransferred;
    private long _chunksDeduplicatedBytes;
    private long _conflictsTotal;
    private long _syncCyclesCompleted;
    private long _syncDurationTicksSum;

    private readonly object _samplesLock = new();
    private readonly List<double> _durationSamples = new();
    private const int MaxDurationSamples = 1000;

    /// <summary>
    /// Total bytes received from remote peers over the network transport.
    /// </summary>
    public long InboundBytesTransferred => Interlocked.Read(ref _inboundBytesTransferred);

    /// <summary>
    /// Total bytes sent to remote peers over the network transport.
    /// </summary>
    public long OutboundBytesTransferred => Interlocked.Read(ref _outboundBytesTransferred);

    /// <summary>
    /// Total bytes transferred across both directions.
    /// </summary>
    public long TotalBytesTransferred => InboundBytesTransferred + OutboundBytesTransferred;

    /// <summary>
    /// Total payload bytes saved via FastCDC chunk deduplication.
    /// </summary>
    public long ChunksDeduplicatedBytes => Interlocked.Read(ref _chunksDeduplicatedBytes);

    /// <summary>
    /// Total concurrent offline edit conflicts detected and resolved.
    /// </summary>
    public long ConflictsTotal => Interlocked.Read(ref _conflictsTotal);

    /// <summary>
    /// Total completed synchronization cycles.
    /// </summary>
    public long SyncCyclesCompleted => Interlocked.Read(ref _syncCyclesCompleted);

    /// <summary>
    /// Cumulative wall-clock duration of all sync cycles in seconds.
    /// </summary>
    public double SyncDurationSecondsSum =>
        (double)Interlocked.Read(ref _syncDurationTicksSum) / TimeSpan.TicksPerSecond;

    /// <summary>
    /// Cumulative delta bandwidth savings percentage (0.0% to 100.0%).
    /// </summary>
    public double BandwidthSavingsRatio
    {
        get
        {
            long dedup = ChunksDeduplicatedBytes;
            long transferred = TotalBytesTransferred;
            long total = dedup + transferred;
            return total > 0 ? ((double)dedup / total) * 100.0 : 0.0;
        }
    }

    /// <inheritdoc />
    public void RecordBytesTransferred(long bytes, bool isOutgoing)
    {
        if (bytes <= 0) return;
        if (isOutgoing)
        {
            Interlocked.Add(ref _outboundBytesTransferred, bytes);
        }
        else
        {
            Interlocked.Add(ref _inboundBytesTransferred, bytes);
        }
    }

    /// <inheritdoc />
    public void RecordChunkDeduplicated(long bytes)
    {
        if (bytes <= 0) return;
        Interlocked.Add(ref _chunksDeduplicatedBytes, bytes);
    }

    /// <inheritdoc />
    public void RecordConflictDetected()
    {
        Interlocked.Increment(ref _conflictsTotal);
    }

    /// <inheritdoc />
    public void RecordSyncCycleCompleted(TimeSpan duration)
    {
        Interlocked.Increment(ref _syncCyclesCompleted);
        Interlocked.Add(ref _syncDurationTicksSum, duration.Ticks);

        double seconds = duration.TotalSeconds;
        lock (_samplesLock)
        {
            if (_durationSamples.Count >= MaxDurationSamples)
            {
                _durationSamples.RemoveAt(0);
            }
            _durationSamples.Add(seconds);
        }
    }

    /// <summary>
    /// Calculates the p95 synchronization cycle duration in seconds.
    /// </summary>
    public double GetP95DurationSeconds()
    {
        lock (_samplesLock)
        {
            if (_durationSamples.Count == 0) return 0.0;
            var sorted = _durationSamples.OrderBy(d => d).ToList();
            int index = (int)Math.Ceiling(0.95 * sorted.Count) - 1;
            return sorted[Math.Clamp(index, 0, sorted.Count - 1)];
        }
    }
}
