using System.Net;
using DeltaSync.Core.Metrics;
using FluentAssertions;
using Xunit;

namespace DeltaSync.Tests.Sync;

public class PrometheusMetricsServerTests
{
    [Fact]
    public async Task MetricsEndpoint_PrometheusScrape_ReturnsSyncCounters_Test()
    {
        // Arrange
        int port = PrometheusMetricsServer.GetAvailablePort();
        var sink = new SyncMetricsSink();
        await using var server = new PrometheusMetricsServer(sink, port);
        server.Start();

        // Simulate transfer of 10MB file with 9MB deduplicated and 1 conflict detected
        const long tenMb = 10L * 1024L * 1024L; // 10,485,760 bytes
        const long nineMb = 9L * 1024L * 1024L; // 9,437,184 bytes

        sink.RecordBytesTransferred(tenMb, isOutgoing: false);
        sink.RecordChunkDeduplicated(nineMb);
        sink.RecordConflictDetected();
        sink.RecordSyncCycleCompleted(TimeSpan.FromMilliseconds(45));

        using var client = new HttpClient();

        // Act: Query /metrics endpoint
        var response = await client.GetAsync(server.MetricsUri);
        var content = await response.Content.ReadAsStringAsync();

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        response.Content.Headers.ContentType?.MediaType.Should().Be("text/plain");

        // Verify standard Prometheus counters
        content.Should().Contain("deltasync_bytes_transferred_total{direction=\"inbound\"} 10485760");
        content.Should().Contain("deltasync_bytes_transferred_total{direction=\"outbound\"} 0");
        content.Should().Contain("deltasync_chunks_deduplicated_total 9437184");
        content.Should().Contain("deltasync_conflicts_total 1");
        content.Should().Contain("deltasync_sync_duration_seconds{quantile=\"0.95\"}");
        content.Should().Contain("deltasync_sync_duration_seconds_count 1");
    }

    [Fact]
    public async Task MetricsServer_UnknownPath_Returns404NotFound_Test()
    {
        // Arrange
        int port = PrometheusMetricsServer.GetAvailablePort();
        var sink = new SyncMetricsSink();
        await using var server = new PrometheusMetricsServer(sink, port);
        server.Start();

        using var client = new HttpClient();

        // Act
        var response = await client.GetAsync($"http://127.0.0.1:{port}/invalid_route");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public void SyncMetricsSink_Calculates_P95AndSavingsRatio_Correctly_Test()
    {
        // Arrange
        var sink = new SyncMetricsSink();

        // 100MB transferred, 900MB deduplicated => 90% savings
        sink.RecordBytesTransferred(100L * 1024 * 1024, isOutgoing: true);
        sink.RecordChunkDeduplicated(900L * 1024 * 1024);

        sink.BandwidthSavingsRatio.Should().BeApproximately(90.0, 0.01);

        // Record durations: 10ms, 20ms, ..., 100ms
        for (int i = 1; i <= 100; i++)
        {
            sink.RecordSyncCycleCompleted(TimeSpan.FromMilliseconds(i));
        }

        // p95 of 1..100 should be 95ms (0.095s)
        sink.GetP95DurationSeconds().Should().BeApproximately(0.095, 0.001);
        sink.SyncCyclesCompleted.Should().Be(100);
    }

    [Fact]
    public async Task SyncMetricsSink_ThreadSafety_RecordsCorrectTotals_Test()
    {
        // Arrange
        var sink = new SyncMetricsSink();
        const int taskCount = 10;
        const int iterationsPerTask = 1000;

        // Act
        var tasks = Enumerable.Range(0, taskCount).Select(_ => Task.Run(() =>
        {
            for (int i = 0; i < iterationsPerTask; i++)
            {
                sink.RecordBytesTransferred(100, isOutgoing: false);
                sink.RecordBytesTransferred(200, isOutgoing: true);
                sink.RecordChunkDeduplicated(50);
                sink.RecordConflictDetected();
                sink.RecordSyncCycleCompleted(TimeSpan.FromMilliseconds(5));
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        // Assert
        sink.InboundBytesTransferred.Should().Be(taskCount * iterationsPerTask * 100L);
        sink.OutboundBytesTransferred.Should().Be(taskCount * iterationsPerTask * 200L);
        sink.ChunksDeduplicatedBytes.Should().Be(taskCount * iterationsPerTask * 50L);
        sink.ConflictsTotal.Should().Be(taskCount * iterationsPerTask);
        sink.SyncCyclesCompleted.Should().Be(taskCount * iterationsPerTask);
    }
}
