using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
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
        var options = CliOptions.Parse(args);
        if (options.ShowHelp)
        {
            PrintUsage(options.PeerTarget);
            return 0;
        }

        return options.Command switch
        {
            CliCommand.Sync => await ExecuteSyncAsync(options),
            CliCommand.Peer => await ExecutePeerAsync(options),
            CliCommand.Conflicts => await ExecuteConflictsAsync(options),
            CliCommand.Status => await ExecuteStatusAsync(options),
            CliCommand.Help => ExecuteHelp(options),
            _ => await ExecuteSyncAsync(options)
        };
    }

    private static int ExecuteHelp(CliOptions options)
    {
        PrintUsage(options.PeerTarget);
        return 0;
    }

    private static async Task<int> ExecuteSyncAsync(CliOptions options)
    {
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

        // Load configured static peers from .deltasync/peers.json and CLI flags
        var configuredPeers = await PeerConfigStore.LoadPeersAsync(options.SyncPath, cts.Token);
        var allStaticEndpoints = configuredPeers.Select(p => p.Endpoint)
            .Concat(options.StaticPeers ?? Array.Empty<string>())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        var staticPeerProvider = new StaticPeerProvider(registry, allStaticEndpoints);

        Guid clusterGuid = Guid.TryParse(options.ClusterId, out var parsedGuid)
            ? parsedGuid
            : new Guid(MD5.HashData(Encoding.UTF8.GetBytes(options.ClusterId)));

        var dialer = new TcpPeerDialer(clusterGuid, options.PeerId, registry);

        await using var coordinator = new PeerConnectionCoordinator(
            clusterGuid,
            options.PeerId,
            options.ListenPort,
            registry,
            dialer: dialer.AsDialer());

        await using var listener = new TcpPeerListener(
            options.PeerId,
            clusterGuid,
            options.ListenPort,
            coordinator);

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

        void LogEvent(string category, string message)
        {
            if (options.IsHeadless)
            {
                Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] [{category}] {message}");
            }
            else
            {
                dashboard.AddEvent(category, message);
            }
        }

        void TriggerPeerConnect(PeerRecord peer)
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await coordinator.ConnectAsync(peer.PeerId, peer.Endpoint, cts.Token);
                }
                catch (OperationCanceledException) when (cts.IsCancellationRequested)
                {
                }
                catch (Exception ex)
                {
                    LogEvent("WARN", $"Failed to connect to peer {peer.PeerId} at {peer.Endpoint}: {ex.Message}");
                }
            });
        }

        registry.PeerDiscovered += (sender, peer) =>
        {
            dashboard.SetActivePeers(registry.ActiveCount);
            LogEvent("PEER", $"Discovered peer {peer.PeerId} at {peer.Endpoint}");
            TriggerPeerConnect(peer);
        };

        coordinator.ConnectionEstablished += (sender, channel) =>
        {
            dashboard.SetActivePeers(coordinator.ActiveConnections.Count);
            LogEvent("PEER", $"Connected to peer {channel.RemotePeerId}");
        };

        coordinator.ConnectionClosed += (sender, peerId) =>
        {
            dashboard.SetActivePeers(coordinator.ActiveConnections.Count);
            LogEvent("PEER", $"Peer {peerId} disconnected");
        };

        watcher.OnFileCreatedOrChanged += (relPath) =>
        {
            dashboard.SetState(SyncEngineState.Syncing);
            LogEvent("SYNC", $"File created/modified: {relPath}");
            return Task.CompletedTask;
        };

        watcher.OnFileDeleted += (relPath) =>
        {
            dashboard.SetState(SyncEngineState.Syncing);
            LogEvent("SYNC", $"File deleted: {relPath}");
            return Task.CompletedTask;
        };

        // Start background synchronization services
        await orchestrator.StartAsync(cts.Token);
        watcher.StartWatching();

        try
        {
            listener.Start();
            LogEvent("NET", $"TCP peer transport listener active on {listener.LocalEndPoint}");
        }
        catch (Exception ex)
        {
            LogEvent("ERROR", $"Failed to bind TCP listener on port {options.ListenPort}: {ex.Message}");
            AnsiConsole.MarkupLine($"[bold red]Error:[/] Failed to bind TCP listener on port {options.ListenPort}: {Markup.Escape(ex.Message)}");
            return 1;
        }

        if (allStaticEndpoints.Count > 0)
        {
            LogEvent("PEER", $"Loaded {allStaticEndpoints.Count} static peer(s): {string.Join(", ", allStaticEndpoints)}");
        }

        // Trigger connection attempts for already discovered/configured static peers
        foreach (var staticPeer in registry.GetActivePeers())
        {
            TriggerPeerConnect(staticPeer);
        }

        LogEvent("INFO", $"DeltaSync active. Monitoring '{options.SyncPath}' on port {listener.Port}.");

        // Run live dashboard or headless loop
        try
        {
            if (options.IsHeadless)
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cts.Token);
            }
            else
            {
                await dashboard.RunAsync(cts.Token);
            }
        }
        catch (OperationCanceledException)
        {
            // Clean exit
        }
        finally
        {
            watcher.StopWatching();
            await orchestrator.StopAsync(CancellationToken.None);
            await listener.StopAsync();
        }

        AnsiConsole.MarkupLine("[bold yellow]DeltaSync daemon stopped cleanly.[/]");
        return 0;
    }

    private static async Task<int> ExecutePeerAsync(CliOptions options)
    {
        switch (options.PeerAction)
        {
            case PeerSubcommand.Add:
                return await ExecutePeerAddAsync(options);
            case PeerSubcommand.Remove:
                return await ExecutePeerRemoveAsync(options);
            case PeerSubcommand.List:
            default:
                return await ExecutePeerListAsync(options);
        }
    }

    private static async Task<int> ExecutePeerAddAsync(CliOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.PeerTarget))
        {
            AnsiConsole.MarkupLine("[bold red]Error:[/] Missing peer endpoint address.");
            AnsiConsole.MarkupLine("Usage: [green]deltasync peer add <host:port>[/] [dim][[--path <dir>]][/]");
            return 1;
        }

        var (success, message, peer) = await PeerConfigStore.AddPeerAsync(options.SyncPath, options.PeerTarget);
        if (!success)
        {
            AnsiConsole.MarkupLine($"[bold red]Error:[/] {Markup.Escape(message)}");
            return 1;
        }

        if (options.JsonOutput)
        {
            Console.WriteLine(JsonSerializer.Serialize(peer, new JsonSerializerOptions { WriteIndented = true }));
            return 0;
        }

        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("[bold]Property[/]");
        table.AddColumn("[bold]Value[/]");
        table.AddRow("Endpoint", $"[bold green]{Markup.Escape(peer!.Endpoint)}[/]");
        table.AddRow("Host / IP", Markup.Escape(peer.Host));
        table.AddRow("Port", peer.Port.ToString());
        table.AddRow("Config File", $"[dim]{Markup.Escape(PeerConfigStore.GetConfigPath(options.SyncPath))}[/]");

        AnsiConsole.Write(new Panel(table)
            .Header("[bold cyan]Peer Configured[/]")
            .Border(BoxBorder.Rounded));
        AnsiConsole.MarkupLine("[bold green]✓ Successfully added static peer endpoint.[/]");
        return 0;
    }

    private static async Task<int> ExecutePeerListAsync(CliOptions options)
    {
        var peers = await PeerConfigStore.LoadPeersAsync(options.SyncPath);

        if (options.JsonOutput)
        {
            Console.WriteLine(JsonSerializer.Serialize(peers, new JsonSerializerOptions { WriteIndented = true }));
            return 0;
        }

        if (peers.Count == 0)
        {
            var emptyPanel = new Panel(new Markup(
                $"[dim]No static peers configured for:[/] [yellow]{Markup.Escape(options.SyncPath)}[/]\n\n" +
                $"Run [green]deltasync peer add <host:port>[/] to configure static peer nodes."))
                .Header("[bold cyan]Configured Peers[/]")
                .Border(BoxBorder.Rounded);
            AnsiConsole.Write(emptyPanel);
            return 0;
        }

        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn(new TableColumn("#").Centered());
        table.AddColumn(new TableColumn("Endpoint").LeftAligned());
        table.AddColumn(new TableColumn("Host / IP").LeftAligned());
        table.AddColumn(new TableColumn("Port").RightAligned());
        table.AddColumn(new TableColumn("Configured (UTC)").Centered());

        for (int i = 0; i < peers.Count; i++)
        {
            var p = peers[i];
            table.AddRow(
                (i + 1).ToString(),
                $"[bold green]{Markup.Escape(p.Endpoint)}[/]",
                Markup.Escape(p.Host),
                p.Port.ToString(),
                p.AddedAt.ToString("yyyy-MM-dd HH:mm:ss"));
        }

        AnsiConsole.Write(new Panel(table)
            .Header($"[bold cyan]Configured Static Peers ({peers.Count})[/]")
            .Border(BoxBorder.Rounded));
        return 0;
    }

    private static async Task<int> ExecutePeerRemoveAsync(CliOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.PeerTarget))
        {
            AnsiConsole.MarkupLine("[bold red]Error:[/] Missing peer endpoint to remove.");
            AnsiConsole.MarkupLine("Usage: [green]deltasync peer remove <host:port>[/] [dim][[--path <dir>]][/]");
            return 1;
        }

        var (success, message) = await PeerConfigStore.RemovePeerAsync(options.SyncPath, options.PeerTarget);
        if (!success)
        {
            AnsiConsole.MarkupLine($"[bold red]Error:[/] {Markup.Escape(message)}");
            return 1;
        }

        AnsiConsole.MarkupLine($"[bold green]✓ {Markup.Escape(message)}[/]");
        return 0;
    }

    private static async Task<int> ExecuteConflictsAsync(CliOptions options)
    {
        if (!Directory.Exists(options.SyncPath))
        {
            AnsiConsole.MarkupLine($"[bold red]Error:[/] Directory '{Markup.Escape(options.SyncPath)}' does not exist.");
            return 1;
        }

        var conflicts = await ConflictScanner.ScanConflictsAsync(options.SyncPath);

        if (options.JsonOutput)
        {
            Console.WriteLine(JsonSerializer.Serialize(conflicts, new JsonSerializerOptions { WriteIndented = true }));
            return 0;
        }

        if (conflicts.Count == 0)
        {
            var panel = new Panel(new Markup(
                $"[bold green]✓ No unresolved conflicts detected.[/]\n\n" +
                $"[dim]Sync Root:[/] {Markup.Escape(options.SyncPath)}\n" +
                $"All files are cleanly converged under ADR-0001 side-by-side branch policy."))
                .Header("[bold cyan]Conflict Inspection[/]")
                .Border(BoxBorder.Rounded);
            AnsiConsole.Write(panel);
            return 0;
        }

        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("[bold]Original File[/]");
        table.AddColumn("[bold]Conflicted Branch (Side-by-Side)[/]");
        table.AddColumn("[bold]Peer Source[/]");
        table.AddColumn("[bold]Sizes (Orig / Conf)[/]");
        table.AddColumn("[bold]Conflict Modified (UTC)[/]");
        table.AddColumn("[bold]Diagnosis[/]");

        foreach (var c in conflicts)
        {
            string origSize = c.OriginalExists ? FormatBytes(c.OriginalSizeBytes) : "[red]missing[/]";
            string confSize = FormatBytes(c.ConflictSizeBytes);

            table.AddRow(
                Markup.Escape(c.RelativeOriginalPath),
                $"[bold yellow]{Markup.Escape(c.RelativeConflictPath)}[/]",
                $"[cyan]{Markup.Escape(c.PeerId)}[/]",
                $"{origSize} / {confSize}",
                c.ConflictModifiedUtc.ToString("yyyy-MM-dd HH:mm:ss"),
                Markup.Escape(c.Reason ?? "Conflict"));
        }

        AnsiConsole.MarkupLine($"[bold red]Found {conflicts.Count} unresolved conflict branch(es)[/] in [dim]{Markup.Escape(options.SyncPath)}[/]:\n");
        AnsiConsole.Write(table);
        AnsiConsole.MarkupLine(
            "\n[dim]Under [bold]ADR-0001[/], concurrent offline edits preserve both files side-by-side to guarantee zero data loss.\n" +
            "To resolve: review both files, preserve your preferred version in the original file, and remove the conflicted branch file.[/]");
        return 0;
    }

    private static async Task<int> ExecuteStatusAsync(CliOptions options)
    {
        if (!Directory.Exists(options.SyncPath))
        {
            AnsiConsole.MarkupLine($"[bold red]Error:[/] Directory '{Markup.Escape(options.SyncPath)}' does not exist.");
            return 1;
        }

        string dbPath = Path.Combine(options.SyncPath, ".deltasync", "state.db");
        bool dbExists = File.Exists(dbPath);
        long dbSizeBytes = dbExists ? new FileInfo(dbPath).Length : 0;

        int trackedFiles = 0;
        long trackedBytes = 0;

        if (dbExists)
        {
            try
            {
                await using var store = new SqliteStateStore(dbPath);
                var files = await store.GetAllFilesAsync(includeDeleted: false);
                trackedFiles = files.Count;
                trackedBytes = files.Sum(f => f.SizeBytes);
            }
            catch
            {
                // DB might be locked or corrupt
            }
        }

        var peers = await PeerConfigStore.LoadPeersAsync(options.SyncPath);
        var conflicts = await ConflictScanner.ScanConflictsAsync(options.SyncPath);

        if (options.JsonOutput)
        {
            var statusDoc = new
            {
                syncPath = options.SyncPath,
                stateStoreExists = dbExists,
                databaseSizeBytes = dbSizeBytes,
                trackedFilesCount = trackedFiles,
                trackedTotalBytes = trackedBytes,
                configuredPeersCount = peers.Count,
                unresolvedConflictsCount = conflicts.Count
            };
            Console.WriteLine(JsonSerializer.Serialize(statusDoc, new JsonSerializerOptions { WriteIndented = true }));
            return 0;
        }

        var table = new Table().Border(TableBorder.Rounded);
        table.AddColumn("[bold]Property[/]");
        table.AddColumn("[bold]Status / Value[/]");

        table.AddRow("Sync Root Path", Markup.Escape(options.SyncPath));
        table.AddRow("State Database", dbExists
            ? $"[green]Connected[/] ({FormatBytes(dbSizeBytes)})"
            : "[dim]Not initialized (will be created on sync)[/]");
        table.AddRow("Tracked Files", $"{trackedFiles:N0} active files ({FormatBytes(trackedBytes)})");
        table.AddRow("Configured Static Peers", $"{peers.Count} peer(s) in .deltasync/peers.json");
        table.AddRow("Conflict Branches", conflicts.Count > 0
            ? $"[bold red]{conflicts.Count} unresolved conflict(s)[/]"
            : "[bold green]0 conflicts (clean)[/]");

        AnsiConsole.Write(new Panel(table)
            .Header("[bold cyan]DeltaSync Repository Status[/]")
            .Border(BoxBorder.Rounded));
        return 0;
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F1} KB";
        if (bytes < 1024 * 1024 * 1024) return $"{bytes / (1024.0 * 1024):F2} MB";
        return $"{bytes / (1024.0 * 1024 * 1024):F2} GB";
    }

    private static void PrintUsage(string? command = null)
    {
        AnsiConsole.MarkupLine("[bold cyan]DeltaSync[/] — Peer-to-Peer File Synchronization Engine (Karpathy LLM Wiki Architecture)");
        AnsiConsole.MarkupLine("High-performance FastCDC chunking, Merkle trie sync, and ADR-0001 side-by-side conflict preservation.\n");

        AnsiConsole.MarkupLine("[bold yellow]USAGE:[/]");
        AnsiConsole.MarkupLine("  deltasync [green]<command>[/] [dim][[options]][/]");
        AnsiConsole.MarkupLine("  deltasync [green]sync[/] [yellow][[<path>]][/] [dim][[options]][/]\n");

        AnsiConsole.MarkupLine("[bold yellow]COMMANDS:[/]");
        AnsiConsole.MarkupLine("  [green]sync[/] [yellow][[<path>]][/]             Start peer-to-peer file synchronization daemon (default)");
        AnsiConsole.MarkupLine("  [green]peer add[/] [yellow]<host:port>[/]      Configure a static peer endpoint to connect to");
        AnsiConsole.MarkupLine("  [green]peer list[/]                   List all configured static peer endpoints");
        AnsiConsole.MarkupLine("  [green]peer remove[/] [yellow]<host:port>[/]   Remove a static peer endpoint from configuration");
        AnsiConsole.MarkupLine("  [green]conflicts[/] [yellow][[<path>]][/]        Scan and display concurrent edit conflicts (ADR-0001)");
        AnsiConsole.MarkupLine("  [green]status[/] [yellow][[<path>]][/]           Display repository state, database footprint, and peers");
        AnsiConsole.MarkupLine("  [green]help[/] [yellow][[<command>]][/]          Show help documentation\n");

        AnsiConsole.MarkupLine("[bold yellow]SYNC OPTIONS:[/]");
        AnsiConsole.MarkupLine("  [yellow]--path <dir>[/]              Synchronization root directory (default: current directory)");
        AnsiConsole.MarkupLine("  [yellow]--port <port>[/]             P2P listen TCP port (default: 4242)");
        AnsiConsole.MarkupLine("  [yellow]--peer-id <id>[/]            Unique peer node identifier (default: auto-generated)");
        AnsiConsole.MarkupLine("  [yellow]--cluster <id>[/]            Cluster network identifier (default: 'default')");
        AnsiConsole.MarkupLine("  [yellow]--metrics-port <p>[/]        Prometheus HTTP metrics listener port (default: 9090)");
        AnsiConsole.MarkupLine("  [yellow]--peer <host:port>[/]        Additional static peer to connect to (can repeat)");
        AnsiConsole.MarkupLine("  [yellow]--headless[/]                Headless mode: disable terminal dashboard and log to stdout");
        AnsiConsole.MarkupLine("  [yellow]--smoke-test[/]              Execute one UI/startup verification cycle and exit 0");
        AnsiConsole.MarkupLine("  [yellow]--json[/]                    Format output as machine-readable JSON");
        AnsiConsole.MarkupLine("  [yellow]--help, -h[/]                Display this help screen\n");

        AnsiConsole.MarkupLine("[bold yellow]EXAMPLES:[/]");
        AnsiConsole.MarkupLine("  deltasync sync ./my-notes --port 4242");
        AnsiConsole.MarkupLine("  deltasync peer add 192.168.1.50:4242");
        AnsiConsole.MarkupLine("  deltasync peer list");
        AnsiConsole.MarkupLine("  deltasync peer remove 192.168.1.50:4242");
        AnsiConsole.MarkupLine("  deltasync conflicts ./my-notes");
        AnsiConsole.MarkupLine("  deltasync status ./my-notes");
    }
}
