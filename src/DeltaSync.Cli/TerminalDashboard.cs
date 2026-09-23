using System.Collections.Concurrent;
using DeltaSync.Core.Metrics;
using Spectre.Console;
using Spectre.Console.Rendering;

namespace DeltaSync.Cli;

public enum SyncEngineState
{
    Idle,
    Syncing,
    Reconciling
}

public sealed record DashboardEvent(DateTimeOffset Timestamp, string Category, string Description);

/// <summary>
/// Live Spectre.Console interactive terminal dashboard for DeltaSync (ADR-0005).
/// Decouples rendering from synchronization worker threads with zero idle CPU spin.
/// </summary>
public sealed class TerminalDashboard
{
    private readonly SyncMetricsSink _metrics;
    private readonly string _localPeerId;
    private readonly string _clusterId;
    private readonly string _syncPath;
    private readonly int _metricsPort;

    private SyncEngineState _state = SyncEngineState.Idle;
    private int _activePeersCount;
    private readonly ConcurrentQueue<DashboardEvent> _recentEvents = new();
    private const int MaxRecentEvents = 8;

    private long _lastBytesTransferred;
    private DateTimeOffset _lastThroughputCheck = DateTimeOffset.UtcNow;
    private double _currentThroughputKbSec;

    public TerminalDashboard(
        SyncMetricsSink metrics,
        string localPeerId,
        string clusterId,
        string syncPath,
        int metricsPort)
    {
        _metrics = metrics ?? throw new ArgumentNullException(nameof(metrics));
        _localPeerId = localPeerId;
        _clusterId = clusterId;
        _syncPath = syncPath;
        _metricsPort = metricsPort;
    }

    /// <summary>
    /// Updates the current synchronization state.
    /// </summary>
    public void SetState(SyncEngineState state)
    {
        _state = state;
    }

    /// <summary>
    /// Updates the count of currently connected peers.
    /// </summary>
    public void SetActivePeers(int count)
    {
        _activePeersCount = Math.Max(0, count);
    }

    /// <summary>
    /// Appends a new timestamped event to the recent events feed.
    /// </summary>
    public void AddEvent(string category, string description)
    {
        _recentEvents.Enqueue(new DashboardEvent(DateTimeOffset.UtcNow, category, description));
        while (_recentEvents.Count > MaxRecentEvents && _recentEvents.TryDequeue(out _))
        {
        }
    }

    /// <summary>
    /// Calculates transfer throughput based on bytes transferred delta.
    /// </summary>
    private void UpdateThroughput()
    {
        var now = DateTimeOffset.UtcNow;
        var elapsed = (now - _lastThroughputCheck).TotalSeconds;
        if (elapsed >= 0.5)
        {
            long currentBytes = _metrics.TotalBytesTransferred;
            long deltaBytes = Math.Max(0, currentBytes - _lastBytesTransferred);
            _currentThroughputKbSec = (deltaBytes / 1024.0) / elapsed;
            _lastBytesTransferred = currentBytes;
            _lastThroughputCheck = now;
        }
    }

    /// <summary>
    /// Builds the composite Spectre.Console renderable layout tree.
    /// </summary>
    public IRenderable BuildLayout()
    {
        UpdateThroughput();

        var stateMarkup = _state switch
        {
            SyncEngineState.Syncing => "[bold green]SYNCING[/]",
            SyncEngineState.Reconciling => "[bold yellow]RECONCILING[/]",
            _ => "[bold grey]IDLE[/]"
        };

        // Header Banner Panel
        var headerTable = new Table().Border(TableBorder.None).HideHeaders().Expand();
        headerTable.AddColumn(new TableColumn("Left").LeftAligned());
        headerTable.AddColumn(new TableColumn("Right").RightAligned());
        headerTable.AddRow(
            new Markup("[bold cyan]DeltaSync[/] [dim]Peer-to-Peer Synchronization Engine[/]"),
            new Markup($"[dim]Cluster:[/] [bold]{_clusterId}[/] | [dim]Node:[/] [green]{_localPeerId}[/]")
        );

        // Status & Config Panel
        var statusGrid = new Grid();
        statusGrid.AddColumn();
        statusGrid.AddColumn();
        statusGrid.AddRow(new Markup("[dim]Sync Path:[/]"), new Markup($"[white]{Markup.Escape(_syncPath)}[/]"));
        statusGrid.AddRow(new Markup("[dim]State:[/]"), new Markup(stateMarkup));
        statusGrid.AddRow(new Markup("[dim]Connected Peers:[/]"), new Markup($"[bold green]{_activePeersCount} active[/]"));
        statusGrid.AddRow(new Markup("[dim]Prometheus Endpoint:[/]"), new Markup($"[link=http://127.0.0.1:{_metricsPort}/metrics]http://127.0.0.1:{_metricsPort}/metrics[/]"));

        var statusPanel = new Panel(statusGrid)
            .Header("[bold blue]Engine Status[/]")
            .Border(BoxBorder.Rounded);

        // Telemetry & Metrics Panel
        var metricsGrid = new Grid();
        metricsGrid.AddColumn();
        metricsGrid.AddColumn();

        double inboundMb = _metrics.InboundBytesTransferred / (1024.0 * 1024.0);
        double outboundMb = _metrics.OutboundBytesTransferred / (1024.0 * 1024.0);
        double dedupMb = _metrics.ChunksDeduplicatedBytes / (1024.0 * 1024.0);
        double savingsRatio = _metrics.BandwidthSavingsRatio;
        double p95Ms = _metrics.GetP95DurationSeconds() * 1000.0;

        metricsGrid.AddRow(new Markup("[dim]Transferred (In/Out):[/]"), new Markup($"[cyan]{inboundMb:F2} MB[/] / [cyan]{outboundMb:F2} MB[/]"));
        metricsGrid.AddRow(new Markup("[dim]Throughput:[/]"), new Markup($"[bold cyan]{_currentThroughputKbSec:F1} KB/s[/]"));
        metricsGrid.AddRow(new Markup("[dim]FastCDC Dedup Saved:[/]"), new Markup($"[bold green]{dedupMb:F2} MB[/]"));
        metricsGrid.AddRow(new Markup("[dim]Bandwidth Savings:[/]"), new Markup($"[bold green]{savingsRatio:F1}%[/]"));
        metricsGrid.AddRow(new Markup("[dim]Conflicts Resolved:[/]"), new Markup($"[yellow]{_metrics.ConflictsTotal}[/] (ADR-0001)"));
        metricsGrid.AddRow(new Markup("[dim]Sync Latency (p95):[/]"), new Markup($"[magenta]{p95Ms:F1} ms[/] ({_metrics.SyncCyclesCompleted} cycles)"));

        var metricsPanel = new Panel(metricsGrid)
            .Header("[bold green]Telemetry & Performance[/]")
            .Border(BoxBorder.Rounded);

        // Events Table Panel
        var eventsTable = new Table().Border(TableBorder.Simple).Expand();
        eventsTable.AddColumn(new TableColumn("[dim]Time[/]").Width(12));
        eventsTable.AddColumn(new TableColumn("[dim]Type[/]").Width(14));
        eventsTable.AddColumn(new TableColumn("[dim]Details[/]"));

        var eventsList = _recentEvents.ToArray();
        if (eventsList.Length == 0)
        {
            eventsTable.AddRow(
                new Markup("[dim]--:--:--[/]"),
                new Markup("[dim]INFO[/]"),
                new Markup("[dim]System initialized. Ready for peer synchronization.[/]")
            );
        }
        else
        {
            foreach (var evt in eventsList)
            {
                var typeMarkup = evt.Category switch
                {
                    "SYNC" => "[green]SYNC[/]",
                    "PEER" => "[cyan]PEER[/]",
                    "CONFLICT" => "[bold yellow]CONFLICT[/]",
                    "ERROR" => "[bold red]ERROR[/]",
                    _ => $"[dim]{evt.Category}[/]"
                };
                eventsTable.AddRow(
                    new Markup($"[dim]{evt.Timestamp:HH:mm:ss.fff}[/]"),
                    new Markup(typeMarkup),
                    new Markup(Markup.Escape(evt.Description))
                );
            }
        }

        var eventsPanel = new Panel(eventsTable)
            .Header("[bold yellow]Recent Sync Events[/]")
            .Border(BoxBorder.Rounded);

        // Top Split Grid (Status + Metrics)
        var topSplit = new Grid();
        topSplit.AddColumn();
        topSplit.AddColumn();
        topSplit.AddRow(statusPanel, metricsPanel);

        // Main Composite Layout
        var mainLayout = new Rows(
            headerTable,
            new Rule().RuleStyle("grey"),
            topSplit,
            eventsPanel
        );

        return mainLayout;
    }

    /// <summary>
    /// Executes the live terminal display loop until cancellation is requested.
    /// Adapts refresh interval to eliminate CPU spin during idle periods.
    /// </summary>
    public async Task RunAsync(CancellationToken ct)
    {
        await AnsiConsole.Live(BuildLayout())
            .AutoClear(false)
            .Overflow(VerticalOverflow.Ellipsis)
            .Cropping(VerticalOverflowCropping.Bottom)
            .StartAsync(async ctx =>
            {
                while (!ct.IsCancellationRequested)
                {
                    ctx.UpdateTarget(BuildLayout());

                    // Adaptive sleep: 100ms when syncing/reconciling, 350ms when idle to guarantee zero CPU spin
                    int delayMs = _state == SyncEngineState.Idle ? 350 : 100;
                    try
                    {
                        await Task.Delay(delayMs, ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }).ConfigureAwait(false);
    }
}
