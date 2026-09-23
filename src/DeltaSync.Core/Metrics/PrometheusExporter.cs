using System.Globalization;
using System.Text;

namespace DeltaSync.Core.Metrics;

/// <summary>
/// Formats sync telemetry metrics into Prometheus text exposition format version 0.0.4.
/// </summary>
public static class PrometheusExporter
{
    public const string ContentType = "text/plain; version=0.0.4; charset=utf-8";

    /// <summary>
    /// Generates the Prometheus text metrics representation for the specified sink.
    /// </summary>
    public static string Export(SyncMetricsSink sink)
    {
        var sb = new StringBuilder();

        // 1. deltasync_bytes_transferred_total
        sb.AppendLine("# HELP deltasync_bytes_transferred_total Total bytes transferred over the network transport.");
        sb.AppendLine("# TYPE deltasync_bytes_transferred_total counter");
        sb.AppendLine(string.Format(CultureInfo.InvariantCulture,
            "deltasync_bytes_transferred_total{{direction=\"inbound\"}} {0}", sink.InboundBytesTransferred));
        sb.AppendLine(string.Format(CultureInfo.InvariantCulture,
            "deltasync_bytes_transferred_total{{direction=\"outbound\"}} {0}", sink.OutboundBytesTransferred));

        // 2. deltasync_chunks_deduplicated_total
        sb.AppendLine("# HELP deltasync_chunks_deduplicated_total Total payload bytes saved via FastCDC chunk deduplication.");
        sb.AppendLine("# TYPE deltasync_chunks_deduplicated_total counter");
        sb.AppendLine(string.Format(CultureInfo.InvariantCulture,
            "deltasync_chunks_deduplicated_total {0}", sink.ChunksDeduplicatedBytes));

        // 3. deltasync_conflicts_total
        sb.AppendLine("# HELP deltasync_conflicts_total Total concurrent offline edit conflicts detected.");
        sb.AppendLine("# TYPE deltasync_conflicts_total counter");
        sb.AppendLine(string.Format(CultureInfo.InvariantCulture,
            "deltasync_conflicts_total {0}", sink.ConflictsTotal));

        // 4. deltasync_sync_duration_seconds
        sb.AppendLine("# HELP deltasync_sync_duration_seconds Synchronization cycle execution duration in seconds.");
        sb.AppendLine("# TYPE deltasync_sync_duration_seconds summary");
        sb.AppendLine(string.Format(CultureInfo.InvariantCulture,
            "deltasync_sync_duration_seconds{{quantile=\"0.95\"}} {0:F4}", sink.GetP95DurationSeconds()));
        sb.AppendLine(string.Format(CultureInfo.InvariantCulture,
            "deltasync_sync_duration_seconds_sum {0:F4}", sink.SyncDurationSecondsSum));
        sb.AppendLine(string.Format(CultureInfo.InvariantCulture,
            "deltasync_sync_duration_seconds_count {0}", sink.SyncCyclesCompleted));

        return sb.ToString();
    }
}
