using DeltaSync.Cli;
using DeltaSync.Core.Metrics;
using FluentAssertions;
using Spectre.Console;
using Xunit;

namespace DeltaSync.Tests.Sync;

public class CliSmokeTests
{
    [Fact]
    public void CliOptions_Parse_HandlesValidArguments_Test()
    {
        // Arrange
        string[] args =
        [
            "--path", @"C:\temp\sync",
            "--port", "5050",
            "--peer-id", "peer-alpha",
            "--cluster", "my-cluster",
            "--metrics-port", "9191",
            "--smoke-test"
        ];

        // Act
        var options = CliOptions.Parse(args);

        // Assert
        options.SyncPath.Should().Be(Path.GetFullPath(@"C:\temp\sync"));
        options.ListenPort.Should().Be(5050);
        options.PeerId.Should().Be("peer-alpha");
        options.ClusterId.Should().Be("my-cluster");
        options.MetricsPort.Should().Be(9191);
        options.IsSmokeTest.Should().BeTrue();
        options.ShowHelp.Should().BeFalse();
    }

    [Fact]
    public void CliOptions_Parse_HandlesHelpFlag_Test()
    {
        var options = CliOptions.Parse(["--help"]);
        options.ShowHelp.Should().BeTrue();

        var optionsShort = CliOptions.Parse(["-h"]);
        optionsShort.ShowHelp.Should().BeTrue();
    }

    [Fact]
    public void TerminalDashboard_BuildLayout_RendersAllPanels_Test()
    {
        // Arrange
        var sink = new SyncMetricsSink();
        sink.RecordBytesTransferred(2 * 1024 * 1024, isOutgoing: false);
        sink.RecordBytesTransferred(1 * 1024 * 1024, isOutgoing: true);
        sink.RecordChunkDeduplicated(18 * 1024 * 1024);
        sink.RecordConflictDetected();
        sink.RecordSyncCycleCompleted(TimeSpan.FromMilliseconds(42));

        var dashboard = new TerminalDashboard(
            sink,
            "test-node",
            "test-cluster",
            @"C:\test\sync",
            9090);

        dashboard.SetState(SyncEngineState.Syncing);
        dashboard.SetActivePeers(3);
        dashboard.AddEvent("SYNC", "Reconciling prefix test/dir");

        // Act
        var layout = dashboard.BuildLayout();

        // Render to in-memory console to verify content
        using var writer = new StringWriter();
        var console = AnsiConsole.Create(new AnsiConsoleSettings
        {
            Out = new AnsiConsoleOutput(writer)
        });
        console.Write(layout);
        var output = writer.ToString();

        // Assert
        output.Should().Contain("DeltaSync");
        output.Should().Contain("test-cluster");
        output.Should().Contain("test-node");
        output.Should().Contain("Engine Status");
        output.Should().Contain("Telemetry & Performance");
        output.Should().Contain("Recent Sync Events");
        output.Should().Contain("SYNCING");
        output.Should().Contain("3 active");
        output.Should().Contain("9090/metrics");
        output.Should().Contain("Reconciling prefix test/dir");
    }

    [Fact]
    public async Task CliProgram_SmokeTest_ReturnsExitCodeZero_Test()
    {
        // Act: Execute CLI smoke-test entrypoint
        int exitCode = await Program.Main(["--smoke-test"]);

        // Assert
        exitCode.Should().Be(0);
    }
}
