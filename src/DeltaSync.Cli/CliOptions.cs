namespace DeltaSync.Cli;

public enum CliCommand
{
    Sync,
    Peer,
    Conflicts,
    Status,
    Help
}

public enum PeerSubcommand
{
    List,
    Add,
    Remove
}

/// <summary>
/// Structured command-line options and parser for DeltaSync CLI.
/// Supports structured commands: deltasync sync [path], deltasync peer add <addr>, deltasync conflicts [path], deltasync status [path].
/// </summary>
public sealed record CliOptions(
    string SyncPath,
    int ListenPort = 4242,
    string PeerId = "",
    string ClusterId = "default",
    int MetricsPort = 9090,
    bool IsSmokeTest = false,
    bool ShowHelp = false,
    CliCommand Command = CliCommand.Sync,
    PeerSubcommand PeerAction = PeerSubcommand.List,
    string? PeerTarget = null,
    IReadOnlyList<string>? StaticPeers = null,
    bool IsHeadless = false,
    bool JsonOutput = false)
{
    public static CliOptions Parse(string[] args)
    {
        string syncPath = Directory.GetCurrentDirectory();
        int listenPort = 4242;
        string peerId = string.Empty;
        string clusterId = "default";
        int metricsPort = 9090;
        bool isSmokeTest = false;
        bool showHelp = false;
        CliCommand command = CliCommand.Sync;
        PeerSubcommand peerAction = PeerSubcommand.List;
        string? peerTarget = null;
        var staticPeers = new List<string>();
        bool isHeadless = false;
        bool jsonOutput = false;

        if (args.Length == 0)
        {
            peerId = $"node-{Guid.NewGuid():N}"[..12];
            return new CliOptions(
                SyncPath: syncPath,
                ListenPort: listenPort,
                PeerId: peerId,
                ClusterId: clusterId,
                MetricsPort: metricsPort,
                IsSmokeTest: isSmokeTest,
                ShowHelp: showHelp,
                Command: command,
                PeerAction: peerAction,
                PeerTarget: peerTarget,
                StaticPeers: staticPeers,
                IsHeadless: isHeadless,
                JsonOutput: jsonOutput);
        }

        int index = 0;
        string first = args[0].Trim();

        // Check for top-level subcommands
        if (string.Equals(first, "sync", StringComparison.OrdinalIgnoreCase))
        {
            command = CliCommand.Sync;
            index++;
            if (index < args.Length && !args[index].StartsWith('-'))
            {
                syncPath = Path.GetFullPath(args[index++]);
            }
        }
        else if (string.Equals(first, "peer", StringComparison.OrdinalIgnoreCase))
        {
            command = CliCommand.Peer;
            index++;
            if (index < args.Length && !args[index].StartsWith('-'))
            {
                string sub = args[index++].ToLowerInvariant();
                switch (sub)
                {
                    case "add":
                        peerAction = PeerSubcommand.Add;
                        if (index < args.Length && !args[index].StartsWith('-'))
                        {
                            peerTarget = args[index++];
                        }
                        break;
                    case "remove":
                    case "rm":
                    case "del":
                        peerAction = PeerSubcommand.Remove;
                        if (index < args.Length && !args[index].StartsWith('-'))
                        {
                            peerTarget = args[index++];
                        }
                        break;
                    case "list":
                    case "ls":
                        peerAction = PeerSubcommand.List;
                        break;
                    default:
                        peerTarget = sub;
                        break;
                }
            }
        }
        else if (string.Equals(first, "conflicts", StringComparison.OrdinalIgnoreCase) ||
                 string.Equals(first, "conflict", StringComparison.OrdinalIgnoreCase))
        {
            command = CliCommand.Conflicts;
            index++;
            if (index < args.Length && !args[index].StartsWith('-'))
            {
                syncPath = Path.GetFullPath(args[index++]);
            }
        }
        else if (string.Equals(first, "status", StringComparison.OrdinalIgnoreCase))
        {
            command = CliCommand.Status;
            index++;
            if (index < args.Length && !args[index].StartsWith('-'))
            {
                syncPath = Path.GetFullPath(args[index++]);
            }
        }
        else if (string.Equals(first, "help", StringComparison.OrdinalIgnoreCase))
        {
            command = CliCommand.Help;
            showHelp = true;
            index++;
            if (index < args.Length && !args[index].StartsWith('-'))
            {
                peerTarget = args[index++];
            }
        }
        else if (!first.StartsWith('-'))
        {
            // Positional directory path for sync, e.g. "deltasync /path/to/dir"
            command = CliCommand.Sync;
            syncPath = Path.GetFullPath(first);
            index++;
        }

        // Parse remaining options/flags
        for (; index < args.Length; index++)
        {
            string arg = args[index];
            switch (arg.ToLowerInvariant())
            {
                case "--path":
                    if (index + 1 < args.Length)
                    {
                        syncPath = Path.GetFullPath(args[++index]);
                    }
                    break;
                case "--port":
                    if (index + 1 < args.Length && int.TryParse(args[++index], out int p))
                    {
                        listenPort = p;
                    }
                    break;
                case "--peer-id":
                case "--node-id":
                    if (index + 1 < args.Length)
                    {
                        peerId = args[++index];
                    }
                    break;
                case "--cluster":
                    if (index + 1 < args.Length)
                    {
                        clusterId = args[++index];
                    }
                    break;
                case "--metrics-port":
                    if (index + 1 < args.Length && int.TryParse(args[++index], out int mp))
                    {
                        metricsPort = mp;
                    }
                    break;
                case "--peer":
                case "--static-peer":
                    if (index + 1 < args.Length)
                    {
                        staticPeers.Add(args[++index]);
                    }
                    break;
                case "--address":
                case "--addr":
                    if (index + 1 < args.Length)
                    {
                        peerTarget = args[++index];
                    }
                    break;
                case "--headless":
                case "--no-dashboard":
                    isHeadless = true;
                    break;
                case "--smoke-test":
                    isSmokeTest = true;
                    break;
                case "--json":
                    jsonOutput = true;
                    break;
                case "--help":
                case "-h":
                case "-?":
                    showHelp = true;
                    break;
            }
        }

        if (string.IsNullOrEmpty(peerId))
        {
            peerId = $"node-{Guid.NewGuid():N}"[..12];
        }

        return new CliOptions(
            SyncPath: syncPath,
            ListenPort: listenPort,
            PeerId: peerId,
            ClusterId: clusterId,
            MetricsPort: metricsPort,
            IsSmokeTest: isSmokeTest,
            ShowHelp: showHelp,
            Command: command,
            PeerAction: peerAction,
            PeerTarget: peerTarget,
            StaticPeers: staticPeers,
            IsHeadless: isHeadless,
            JsonOutput: jsonOutput);
    }
}
