using System.Diagnostics;
using System.Net.Sockets;
using System.Text.RegularExpressions;
using FluentAssertions;
using Xunit;

namespace DeltaSync.Tests.Harness;

public class MultiProcessCliHarnessTests
{
    private static readonly Regex AnsiRegex = new(@"\x1B\[[0-9;]*[a-zA-Z]", RegexOptions.Compiled);

    [Fact]
    public void AllocateDistinctPorts_ReturnsFourMutuallyExclusivePorts_Test()
    {
        // Act
        var (p1, p2, p3, p4) = MultiProcessCliHarness.AllocateDistinctPorts();

        // Assert
        var ports = new[] { p1, p2, p3, p4 };
        ports.Should().OnlyHaveUniqueItems("all 4 allocated ports must be mutually exclusive");
        foreach (var port in ports)
        {
            port.Should().BeInRange(1024, 65535, "allocated ports must be in dynamic high port range");
        }
    }

    [Fact]
    public void FindCliDll_LocatesCompiledAssembly_Test()
    {
        // Act
        string cliDll = MultiProcessCliHarness.FindCliDll();

        // Assert
        File.Exists(cliDll).Should().BeTrue($"DeltaSync.Cli.dll must exist at {cliDll}");
        Path.GetFileName(cliDll).Should().Be("DeltaSync.Cli.dll");
    }

    [Fact]
    public async Task WaitForReadinessAsync_UnreachablePort_ThrowsTimeoutException_Test()
    {
        using var httpClient = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };
        var act = async () =>
        {
            await MultiProcessCliHarness.WaitForReadinessAsync(
                httpClient,
                metricsPort: 59998,
                timeout: TimeSpan.FromMilliseconds(150),
                pollInterval: TimeSpan.FromMilliseconds(30));
        };

        var ex = await Assert.ThrowsAsync<TimeoutException>(act);
        ex.Message.Should().Contain("Failed to reach HTTP metrics endpoint on port 59998");
    }

    [Fact]
    public async Task MultiProcessCliHarness_Lifecycle_SpawnsTwoNodes_GatesReadiness_AndDisposesCleanly_Test()
    {
        // Arrange
        MultiProcessCliHarness? harness = null;
        string rootDir = string.Empty;
        Process procA;
        Process procB;

        try
        {
            // Act 1: Spawn two CLI processes with readiness gating
            harness = await MultiProcessCliHarness.StartAsync(new CliHarnessOptions
            {
                StartupTimeout = TimeSpan.FromSeconds(5.0),
                HttpPollInterval = TimeSpan.FromMilliseconds(50),
                DeleteOnDispose = true,
                AutoPeer = true
            });

            // Assert 1: Harness properties and directories
            harness.Should().NotBeNull();
            rootDir = harness.RootDirectory;
            Directory.Exists(rootDir).Should().BeTrue();
            Directory.Exists(harness.NodeA.WorkingDirectory).Should().BeTrue();
            Directory.Exists(harness.NodeB.WorkingDirectory).Should().BeTrue();

            procA = harness.NodeA.Process;
            procB = harness.NodeB.Process;

            procA.HasExited.Should().BeFalse("Node A process should be actively running");
            procB.HasExited.Should().BeFalse("Node B process should be actively running");

            // Assert 2: Verify both HTTP metrics endpoints report 200 OK with Prometheus metrics
            using var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(1) };

            var respA = await httpClient.GetAsync($"http://127.0.0.1:{harness.NodeA.MetricsPort}/metrics");
            respA.IsSuccessStatusCode.Should().BeTrue("Node A metrics endpoint must return HTTP 200 OK");
            string bodyA = await respA.Content.ReadAsStringAsync();
            bodyA.Should().Contain("deltasync_");

            var respB = await httpClient.GetAsync($"http://127.0.0.1:{harness.NodeB.MetricsPort}/metrics");
            respB.IsSuccessStatusCode.Should().BeTrue("Node B metrics endpoint must return HTTP 200 OK");
            string bodyB = await respB.Content.ReadAsStringAsync();
            bodyB.Should().Contain("deltasync_");

            // Assert 3: Headless standard output must contain zero ANSI escapes
            string outA = harness.NodeA.GetAllOutput();
            string outB = harness.NodeB.GetAllOutput();

            AnsiRegex.IsMatch(outA).Should().BeFalse("Node A logs must contain 0 ANSI escape sequences");
            AnsiRegex.IsMatch(outB).Should().BeFalse("Node B logs must contain 0 ANSI escape sequences");

            outA.Should().Contain("[METRICS] Prometheus scrape server listening");
            outA.Should().Contain("[NET] TCP peer transport listener active");
            outB.Should().Contain("[METRICS] Prometheus scrape server listening");
            outB.Should().Contain("[NET] TCP peer transport listener active");

            // Act 2: Invoke dispose
            await harness.DisposeAsync();

            // Assert 4: Both processes terminate and directories are deleted
            harness.NodeA.HasExited.Should().BeTrue("Node A must be terminated after harness disposal");
            harness.NodeB.HasExited.Should().BeTrue("Node B must be terminated after harness disposal");

            Directory.Exists(rootDir).Should().BeFalse("Root directory must be cleanly deleted upon disposal");
        }
        finally
        {
            if (harness != null)
            {
                await harness.DisposeAsync();
            }
            if (!string.IsNullOrEmpty(rootDir) && Directory.Exists(rootDir))
            {
                try { Directory.Delete(rootDir, recursive: true); } catch { }
            }
        }
    }

    [Fact]
    public async Task MultiProcessCliHarness_PrematureProcessExit_FailsPromptlyWithDiagnostics_Test()
    {
        // Act & Assert: Passing --help causes the child process to exit immediately with code 0 instead of running daemon
        var act = async () =>
        {
            await MultiProcessCliHarness.StartAsync(new CliHarnessOptions
            {
                StartupTimeout = TimeSpan.FromSeconds(5.0),
                HttpPollInterval = TimeSpan.FromMilliseconds(50),
                ExtraArgsNodeA = ["--help"]
            });
        };

        // Assert: Should throw InvalidOperationException fast without hanging for 5 seconds
        var sw = Stopwatch.StartNew();
        var ex = await Assert.ThrowsAnyAsync<Exception>(act);
        sw.Stop();

        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(4.5), "should detect premature exit fast before timeout");
        ex.Message.Should().Contain("exited prematurely", "exception must mention premature exit");
    }

    [Fact]
    public async Task MultiProcessCliHarness_PreserveDirectories_WhenDeleteOnDisposeFalse_Test()
    {
        string rootDir = string.Empty;
        MultiProcessCliHarness? harness = null;

        try
        {
            harness = await MultiProcessCliHarness.StartAsync(new CliHarnessOptions
            {
                StartupTimeout = TimeSpan.FromSeconds(5.0),
                DeleteOnDispose = false
            });

            rootDir = harness.RootDirectory;
            Directory.Exists(rootDir).Should().BeTrue();

            await harness.DisposeAsync();

            // When DeleteOnDispose is false, directory is retained
            Directory.Exists(rootDir).Should().BeTrue("directory should be preserved when DeleteOnDispose is false");
        }
        finally
        {
            if (harness != null)
            {
                await harness.DisposeAsync();
            }
            if (!string.IsNullOrEmpty(rootDir) && Directory.Exists(rootDir))
            {
                try { Directory.Delete(rootDir, recursive: true); } catch { }
            }
        }
    }
}
