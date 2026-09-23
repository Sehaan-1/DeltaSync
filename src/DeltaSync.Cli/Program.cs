using System.Net;
using System.Security.Cryptography;
using System.Text;
using DeltaSync.Cli;
using DeltaSync.Core.Metrics;
using DeltaSync.Core.Storage;
using DeltaSync.Core.Sync;
using DeltaSync.Network;
using Spectre.Console;

namespace DeltaSync.Cli;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        // Parse CLI arguments
        var options = CliOptions.Parse(args);
        if (options.ShowHelp)
        {
            PrintUsage();
            return 0;
        }

        var metricsSink = new SyncMetricsSink();

        // Smoke-test mode for CI and test verification
        if (options.IsSmokeTest)
        {
            var testDashboard = new TerminalDashboard(
                metricsSink,
                options.PeerId,
                options.ClusterId,
                options.SyncPath,
                options.MetricsPort);

            testDashboard.SetState(SyncEngineState.Idle);
            testDashboard.SetActivePeers(1);
            testDashboard.AddEvent("INFO", "Smoke test execution initiated.");
            testDashboard.AddEvent("SYNC", "Validated terminal dashboard rendering layout.");

            // Populate sample metrics
            metricsSink.RecordBytesTransferred(1024 * 1024, isOutgoing: false);
            metricsSink.RecordChunkDeduplicated(9 * 1024 * 1024);
            metricsSink.RecordConflictDetected();
            metricsSink.RecordSyncCycleCompleted(TimeSpan.FromMilliseconds(25));

            AnsiConsole.Write(testDashboard.BuildLayout());
            AnsiConsole.MarkupLine("[bold green]DeltaSync CLI Smoke Test Passed Successfully.[/]");
            return 0;
        }

        using var cts = new CancellationTokenSource();
        Console.CancelKeyPress += (sender, eventArgs) =>
        {
            eventArgs.Cancel = true;
            cts.Cancel();
        };

        var dashboard = new TerminalDashboard(
            metricsSink,
            options.PeerId,
            options.ClusterId,
            options.SyncPath,
            options.MetricsPort);

        await using var metricsServer = new PrometheusMetricsServer(metricsSink, options.MetricsPort);
        try
        {
            metricsServer.Start();
            dashboard.AddEvent("METRICS", $"Prometheus scrape server listening on http://127.0.0.1:{options.MetricsPort}/metrics");
        }
        catch (Exception ex)
        {
            dashboard.AddEvent("WARN", $"Prometheus server warning: {ex.Message}");
        }

        // Ensure directories exist
        Directory.CreateDirectory(options.SyncPath);
        string dbPath = Path.Combine(options.SyncPath, ".deltasync", "state.db");
        Directory.CreateDirectory(Path.GetDirectoryName(dbPath)!);

        await using var store = new SqliteStateStore(dbPath);
        var chunkProvider = new SqliteLocalChunkProvider(store, options.SyncPath);

        await using var watcher = new FileWatcherService(options.SyncPath, store, options.PeerId);
        var registry = new PeerRegistry();

        Guid clusterGuid = Guid.TryParse(options.ClusterId, out var parsedGuid)
            ? parsedGuid
            : new Guid(MD5.HashData(Encoding.UTF8.GetBytes(options.ClusterId)));

        await using var coordinator = new PeerConnectionCoordinator(
            clusterGuid,
            options.PeerId,
            options.ListenPort,
            registry);

        var wireProtocol = new SyncWireProtocol(
            store,
            chunkProvider,
            options.SyncPath,
            metricsSink);

        var orchestrator = new SyncOrchestrator(
            options.SyncPath,
            store,
            wireProtocol,
            watcher,
            peerProvider: coordinator,
            metricsSink: metricsSink,
            localPeerId: options.PeerId);

        registry.PeerDiscovered += (sender, peer) =>
        {
            dashboard.SetActivePeers(registry.ActiveCount);
            dashboard.AddEvent("PEER", $"Discovered peer {peer.PeerId} at {peer.Endpoint}");
        };

        coordinator.ConnectionEstablished += (sender, channel) =>
        {
            dashboard.SetActivePeers(coordinator.ActiveConnections.Count);
            dashboard.AddEvent("PEER", $"Connected to peer {channel.RemotePeerId}");
        };

        coordinator.ConnectionClosed += (sender, peerId) =>
        {
            dashboard.SetActivePeers(coordinator.ActiveConnections.Count);
            dashboard.AddEvent("PEER", $"Peer {peerId} disconnected");
        };

        watcher.OnFileCreatedOrChanged += (relPath) =>
        {
            dashboard.SetState(SyncEngineState.Syncing);
            dashboard.AddEvent("SYNC", $"File created/modified: {relPath}");
            return Task.CompletedTask;
        };

        watcher.OnFileDeleted += (relPath) =>
        {
            dashboard.SetState(SyncEngineState.Syncing);
            dashboard.AddEvent("SYNC", $"File deleted: {relPath}");
            return Task.CompletedTask;
        };

        // Start background synchronization services
        await orchestrator.StartAsync(cts.Token);
        watcher.StartWatching();

        dashboard.AddEvent("INFO", $"DeltaSync active. Monitoring '{options.SyncPath}' on port {options.ListenPort}.");

        // Run live dashboard
        try
        {
            await dashboard.RunAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Clean exit
        }
        finally
        {
            watcher.StopWatching();
            await orchestrator.StopAsync(CancellationToken.None);
        }

        AnsiConsole.MarkupLine("[bold yellow]DeltaSync daemon stopped cleanly.[/]");
        return 0;
    }

    private static void PrintUsage()
    {
        AnsiConsole.MarkupLine("[bold cyan]DeltaSync[/] — Peer-to-Peer File Synchronization Engine");
        AnsiConsole.MarkupLine("Usage: [green]dotnet run[/] [dim][[options]][/]\n");
        AnsiConsole.MarkupLine("Options:");
        AnsiConsole.MarkupLine("  [yellow]--path <dir>[/]          Path to local synchronization root (default: current directory)");
        AnsiConsole.MarkupLine("  [yellow]--port <port>[/]         P2P listen TCP port (default: 4242)");
        AnsiConsole.MarkupLine("  [yellow]--peer-id <id>[/]        Unique peer node ID (default: auto-generated)");
        AnsiConsole.MarkupLine("  [yellow]--cluster <id>[/]        Cluster network identifier (default: 'default')");
        AnsiConsole.MarkupLine("  [yellow]--metrics-port <p>[/]    Prometheus HTTP metrics listener port (default: 9090)");
        AnsiConsole.MarkupLine("  [yellow]--smoke-test[/]          Executes one UI cycle and exits immediately");
        AnsiConsole.MarkupLine("  [yellow]--help, -h[/]            Displays this help documentation");
    }
}

public sealed record CliOptions(
    string SyncPath,
    int ListenPort,
    string PeerId,
    string ClusterId,
    int MetricsPort,
    bool IsSmokeTest,
    bool ShowHelp)
{
    public static CliOptions Parse(string[] args)
    {
        string syncPath = Directory.GetCurrentDirectory();
        int listenPort = 4242;
        string peerId = $"node-{Guid.NewGuid():N}"[..12];
        string clusterId = "default";
        int metricsPort = 9090;
        bool isSmokeTest = false;
        bool showHelp = false;

        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i].ToLowerInvariant())
            {
                case "--path" when i + 1 < args.Length:
                    syncPath = Path.GetFullPath(args[++i]);
                    break;
                case "--port" when i + 1 < args.Length && int.TryParse(args[i + 1], out int p):
                    listenPort = p;
                    i++;
                    break;
                case "--peer-id" when i + 1 < args.Length:
                    peerId = args[++i];
                    break;
                case "--cluster" when i + 1 < args.Length:
                    clusterId = args[++i];
                    break;
                case "--metrics-port" when i + 1 < args.Length && int.TryParse(args[i + 1], out int mp):
                    metricsPort = mp;
                    i++;
                    break;
                case "--smoke-test":
                    isSmokeTest = true;
                    break;
                case "--help" or "-h":
                    showHelp = true;
                    break;
            }
        }

        return new CliOptions(syncPath, listenPort, peerId, clusterId, metricsPort, isSmokeTest, showHelp);
    }
}
