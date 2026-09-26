using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using DeltaSync.Cli;
using FluentAssertions;
using Xunit;

namespace DeltaSync.Tests.Sync;

public class CliHeadlessExecutionTests
{
    private static readonly Regex AnsiRegex = new(@"\x1B\[[0-9;]*[a-zA-Z]", RegexOptions.Compiled);
    private static readonly Regex StructuredLogLineRegex = new(@"^\[\d{2}:\d{2}:\d{2}\.\d{3}\] \[[A-Z]+\] .+$", RegexOptions.Compiled);

    private static int GetEphemeralPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }

    [Fact]
    public void CliOptions_Parse_HeadlessAndPeerFlags_ParsedCorrectly_Test()
    {
        // 1. --headless flag
        var optsHeadless = CliOptions.Parse(["--headless"]);
        optsHeadless.IsHeadless.Should().BeTrue();

        // 2. --no-dashboard alias
        var optsNoDash = CliOptions.Parse(["--no-dashboard"]);
        optsNoDash.IsHeadless.Should().BeTrue();

        // 3. Repeated --peer flags
        var optsPeers = CliOptions.Parse(["--peer", "127.0.0.1:4242", "--peer", "192.168.1.100:5050"]);
        optsPeers.StaticPeers.Should().NotBeNull();
        optsPeers.StaticPeers.Should().BeEquivalentTo(new[] { "127.0.0.1:4242", "192.168.1.100:5050" });

        // 4. Comma-separated --peer
        var optsComma = CliOptions.Parse(["--peer", "10.0.0.1:4000,10.0.0.2:4001"]);
        optsComma.StaticPeers.Should().BeEquivalentTo(new[] { "10.0.0.1:4000", "10.0.0.2:4001" });

        // 5. --peer=value syntax
        var optsEq = CliOptions.Parse(["--peer=127.0.0.1:9999", "--static-peer=127.0.0.1:9998"]);
        optsEq.StaticPeers.Should().BeEquivalentTo(new[] { "127.0.0.1:9999", "127.0.0.1:9998" });
    }

    [Fact]
    public async Task CliHeadless_SmokeTest_InProcess_OutputsStructuredLogsWithoutAnsi_Test()
    {
        // Arrange
        var originalOut = Console.Out;
        using var writer = new StringWriter();
        Console.SetOut(writer);

        try
        {
            // Act: Execute in-process smoke test with --headless
            int exitCode = await Program.Main(["--headless", "--smoke-test"]);

            // Assert
            exitCode.Should().Be(0);

            string output = writer.ToString().Trim();
            output.Should().NotBeNullOrWhiteSpace();

            // Zero ANSI escape sequences
            AnsiRegex.IsMatch(output).Should().BeFalse("headless mode must not emit ANSI escape sequences");

            // Required structured log content
            output.Should().Contain("[INFO] Smoke test execution initiated (headless mode).");
            output.Should().Contain("[METRICS] Metrics scrape endpoint configured:");
            output.Should().Contain("[NET] P2P transport listener port: 4242");
            output.Should().Contain("[INFO] DeltaSync CLI Smoke Test Passed Successfully.");

            // Every line matches structured log line format: [TIMESTAMP] [CATEGORY] [MESSAGE]
            string[] lines = output.Split(new[] { "\r\n", "\r", "\n" }, StringSplitOptions.RemoveEmptyEntries);
            lines.Should().HaveCountGreaterThanOrEqualTo(4);
            foreach (var line in lines)
            {
                StructuredLogLineRegex.IsMatch(line).Should().BeTrue($"line '{line}' must match '[TIMESTAMP] [CATEGORY] [MESSAGE]'");
            }
        }
        finally
        {
            Console.SetOut(originalOut);
        }
    }

    [Fact]
    public async Task CliHeadless_SmokeTest_WithStaticPeers_LogsSeededPeers_Test()
    {
        // Arrange
        var originalOut = Console.Out;
        using var writer = new StringWriter();
        Console.SetOut(writer);

        try
        {
            // Act
            int exitCode = await Program.Main([
                "--headless",
                "--smoke-test",
                "--peer", "127.0.0.1:5001",
                "--peer", "127.0.0.1:5002"
            ]);

            // Assert
            exitCode.Should().Be(0);

            string output = writer.ToString().Trim();
            AnsiRegex.IsMatch(output).Should().BeFalse();
            output.Should().Contain("[PEER] Configured static peers: 127.0.0.1:5001, 127.0.0.1:5002");
        }
        finally
        {
            Console.SetOut(originalOut);
        }
    }

    [Fact]
    public async Task CliHeadless_NoDashboardAlias_ActivatesHeadlessSmokeTest_Test()
    {
        // Arrange
        var originalOut = Console.Out;
        using var writer = new StringWriter();
        Console.SetOut(writer);

        try
        {
            // Act
            int exitCode = await Program.Main(["--no-dashboard", "--smoke-test"]);

            // Assert
            exitCode.Should().Be(0);
            string output = writer.ToString().Trim();
            AnsiRegex.IsMatch(output).Should().BeFalse();
            output.Should().Contain("(headless mode)");
        }
        finally
        {
            Console.SetOut(originalOut);
        }
    }

    [Fact]
    public async Task CliHeadless_PhysicalProcess_Execution_SucceedsWithZeroAnsi_Test()
    {
        // Find CLI binary
        string cliDllPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "DeltaSync.Cli.dll");
        if (!File.Exists(cliDllPath))
        {
            cliDllPath = Path.GetFullPath(Path.Combine(
                AppDomain.CurrentDomain.BaseDirectory,
                "..", "..", "..", "..",
                "src", "DeltaSync.Cli", "bin", "Debug", "net8.0", "DeltaSync.Cli.dll"));
        }

        File.Exists(cliDllPath).Should().BeTrue($"DeltaSync.Cli.dll must exist at {cliDllPath}");

        var psi = new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = $"\"{cliDllPath}\" --headless --smoke-test --port 54321 --metrics-port 54322 --peer 127.0.0.1:54320",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using var process = Process.Start(psi)!;
        process.Should().NotBeNull();

        string stdOut = await process.StandardOutput.ReadToEndAsync();
        string stdErr = await process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();

        process.ExitCode.Should().Be(0, $"stderr: {stdErr}");
        stdErr.Should().BeEmpty();

        AnsiRegex.IsMatch(stdOut).Should().BeFalse("physical process standard output in headless mode must contain zero ANSI escapes");
        stdOut.Should().Contain("[INFO] Smoke test execution initiated (headless mode).");
        stdOut.Should().Contain("[NET] P2P transport listener port: 54321");
        stdOut.Should().Contain("[METRICS] Metrics scrape endpoint configured: http://127.0.0.1:54322/metrics");
        stdOut.Should().Contain("[PEER] Configured static peers: 127.0.0.1:54320");
        stdOut.Should().Contain("[INFO] DeltaSync CLI Smoke Test Passed Successfully.");
    }

    [Fact]
    public async Task CliHeadless_LiveDaemon_StartsAndStopsCleanly_WithStructuredLogs_Test()
    {
        int listenPort = GetEphemeralPort();
        int metricsPort = GetEphemeralPort();

        string tempDir = Path.Combine(Path.GetTempPath(), $"deltasync-test-live-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        var originalOut = Console.Out;
        using var writer = new StringWriter();
        Console.SetOut(writer);

        try
        {
            // Act: Run real sync daemon in headless mode for 500ms then cancel
            int exitCode = await Program.RunAsync([
                "--headless",
                "--path", tempDir,
                "--port", listenPort.ToString(),
                "--metrics-port", metricsPort.ToString(),
                "--peer", "127.0.0.1:59999"
            ], cts.Token);

            exitCode.Should().Be(0);

            string output = writer.ToString();
            AnsiRegex.IsMatch(output).Should().BeFalse("headless mode must emit 0 ANSI escape sequences");
            output.Should().Contain("[METRICS] Prometheus scrape server listening on http://127.0.0.1:" + metricsPort + "/metrics");
            output.Should().Contain("[NET] TCP peer transport listener active on");
            output.Should().Contain("[PEER]");
            output.Should().Contain("[INFO] DeltaSync active.");
            output.Should().Contain("[INFO] DeltaSync daemon stopped cleanly.");
        }
        finally
        {
            Console.SetOut(originalOut);
            try
            {
                if (Directory.Exists(tempDir))
                {
                    Directory.Delete(tempDir, recursive: true);
                }
            }
            catch
            {
                // Best effort
            }
        }
    }

    [Fact]
    public async Task CliHeadless_PortCollision_ReturnsExitCodeOneWithoutAnsi_Test()
    {
        // Reserve an exclusive port to force a collision
        using var blocker = new TcpListener(IPAddress.Any, 0);
        blocker.Start();
        int conflictingPort = ((IPEndPoint)blocker.LocalEndpoint).Port;

        string tempDir = Path.Combine(Path.GetTempPath(), $"deltasync-test-collision-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);

        var originalOut = Console.Out;
        using var writer = new StringWriter();
        Console.SetOut(writer);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        try
        {
            // Act: Run sync in headless mode on the occupied port
            int exitCode = await Program.RunAsync([
                "--headless",
                "--path", tempDir,
                "--port", conflictingPort.ToString(),
                "--metrics-port", GetEphemeralPort().ToString()
            ], cts.Token);

            // Assert
            exitCode.Should().Be(1);

            string output = writer.ToString();
            AnsiRegex.IsMatch(output).Should().BeFalse("error logs in headless mode must not contain ANSI escape sequences");
            output.Should().Contain("[ERROR] Failed to bind TCP listener on port " + conflictingPort);
        }
        finally
        {
            Console.SetOut(originalOut);
            blocker.Stop();
            try
            {
                if (Directory.Exists(tempDir))
                {
                    Directory.Delete(tempDir, recursive: true);
                }
            }
            catch
            {
                // Best effort cleanup
            }
        }
    }
}
