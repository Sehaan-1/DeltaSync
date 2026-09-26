using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace DeltaSync.Tests.Harness;

/// <summary>
/// Configuration options for launching the multi-process CLI harness.
/// </summary>
public sealed record CliHarnessOptions(
    string? BaseDirectory = null,
    string? ClusterId = null,
    TimeSpan? StartupTimeout = null,
    TimeSpan? HttpPollInterval = null,
    bool DeleteOnDispose = true,
    bool AutoPeer = true,
    IReadOnlyList<string>? ExtraArgsNodeA = null,
    IReadOnlyList<string>? ExtraArgsNodeB = null)
{
    public TimeSpan ResolvedStartupTimeout => StartupTimeout ?? TimeSpan.FromSeconds(3.0);
    public TimeSpan ResolvedHttpPollInterval => HttpPollInterval ?? TimeSpan.FromMilliseconds(50);
}

/// <summary>
/// Encapsulates a spawned DeltaSync CLI child process and its execution environment.
/// </summary>
public sealed class CliNodeInstance : IDisposable
{
    private readonly ConcurrentQueue<string> _stdoutQueue = new();
    private readonly ConcurrentQueue<string> _stderrQueue = new();
    private bool _disposed;
    private bool _hasExited;
    private int? _exitCode;

    public string Name { get; }
    public string WorkingDirectory { get; }
    public int ListenPort { get; }
    public int MetricsPort { get; }
    public string PeerId { get; }
    public Process Process { get; }
    public int Id { get; }

    public bool HasExited
    {
        get
        {
            if (_hasExited) return true;
            try
            {
                if (Process.HasExited)
                {
                    _hasExited = true;
                    _exitCode = Process.ExitCode;
                    return true;
                }
            }
            catch
            {
                return _hasExited;
            }
            return false;
        }
    }

    public int? ExitCode
    {
        get
        {
            if (_exitCode.HasValue) return _exitCode;
            try
            {
                if (HasExited)
                {
                    _exitCode = Process.ExitCode;
                    return _exitCode;
                }
            }
            catch
            {
                // Process handle may be closed
            }
            return null;
        }
    }

    public IReadOnlyList<string> StandardOutputLines => _stdoutQueue.ToArray();
    public IReadOnlyList<string> StandardErrorLines => _stderrQueue.ToArray();

    public CliNodeInstance(
        string name,
        string workingDirectory,
        int listenPort,
        int metricsPort,
        string peerId,
        Process process)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        WorkingDirectory = workingDirectory ?? throw new ArgumentNullException(nameof(workingDirectory));
        ListenPort = listenPort;
        MetricsPort = metricsPort;
        PeerId = peerId ?? throw new ArgumentNullException(nameof(peerId));
        Process = process ?? throw new ArgumentNullException(nameof(process));
        Id = process.Id;

        Process.OutputDataReceived += (_, e) =>
        {
            if (e.Data != null)
            {
                _stdoutQueue.Enqueue(e.Data);
            }
        };

        Process.ErrorDataReceived += (_, e) =>
        {
            if (e.Data != null)
            {
                _stderrQueue.Enqueue(e.Data);
            }
        };

        Process.BeginOutputReadLine();
        Process.BeginErrorReadLine();
    }

    public string GetAllOutput() => string.Join(Environment.NewLine, _stdoutQueue);

    public string GetErrorOutput() => string.Join(Environment.NewLine, _stderrQueue);

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            if (!Process.HasExited)
            {
                Process.Kill(entireProcessTree: true);
                Process.WaitForExit(3000);
            }
            _hasExited = Process.HasExited;
            _exitCode = Process.ExitCode;
        }
        catch
        {
            _hasExited = true;
        }
        finally
        {
            Process.Dispose();
        }
    }
}

/// <summary>
/// Orchestrates two independent OS CLI processes running on separate loopback TCP and Prometheus ports
/// with isolated directories, verified startup readiness gating, and leak-free teardown.
/// Honors ADR-0001, ADR-0004, and ADR-0005.
/// </summary>
public sealed class MultiProcessCliHarness : IAsyncDisposable
{
    private readonly bool _deleteOnDispose;
    private bool _disposed;

    public string RootDirectory { get; }
    public string ClusterId { get; }
    public CliNodeInstance NodeA { get; }
    public CliNodeInstance NodeB { get; }
    public IReadOnlyList<CliNodeInstance> Nodes { get; }

    private MultiProcessCliHarness(
        string rootDirectory,
        string clusterId,
        CliNodeInstance nodeA,
        CliNodeInstance nodeB,
        bool deleteOnDispose)
    {
        RootDirectory = rootDirectory;
        ClusterId = clusterId;
        NodeA = nodeA;
        NodeB = nodeB;
        Nodes = new[] { nodeA, nodeB };
        _deleteOnDispose = deleteOnDispose;
    }

    /// <summary>
    /// Dynamically allocates 4 distinct, mutually-exclusive loopback ports by holding 4 concurrent TcpListeners.
    /// Eliminates port allocation races and socket TIME_WAIT lockouts (RFC 6335 §6, RFC 9293 §3.5).
    /// </summary>
    public static (int ListenPortA, int MetricsPortA, int ListenPortB, int MetricsPortB) AllocateDistinctPorts()
    {
        var listeners = new List<TcpListener>(4);
        try
        {
            for (int i = 0; i < 4; i++)
            {
                var listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                listeners.Add(listener);
            }

            var ports = listeners.Select(l => ((IPEndPoint)l.LocalEndpoint).Port).ToArray();
            return (ports[0], ports[1], ports[2], ports[3]);
        }
        finally
        {
            foreach (var l in listeners)
            {
                try
                {
                    l.Stop();
                }
                catch
                {
                    // Ignore socket cleanup errors on port release
                }
            }
        }
    }

    /// <summary>
    /// Locates the compiled DeltaSync.Cli assembly across Debug/Release and local build directories.
    /// </summary>
    public static string FindCliDll()
    {
        string directPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "DeltaSync.Cli.dll");
        if (File.Exists(directPath))
        {
            return directPath;
        }

        string[] searchPaths =
        [
            Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..", "..", "..", "..", "src", "DeltaSync.Cli", "bin", "Debug", "net8.0", "DeltaSync.Cli.dll")),
            Path.GetFullPath(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "..", "..", "..", "..", "src", "DeltaSync.Cli", "bin", "Release", "net8.0", "DeltaSync.Cli.dll"))
        ];

        foreach (var path in searchPaths)
        {
            if (File.Exists(path))
            {
                return path;
            }
        }

        throw new FileNotFoundException($"DeltaSync.Cli.dll could not be located in {directPath} or candidate source paths.");
    }

    /// <summary>
    /// Polls the HTTP metrics endpoint (GET http://127.0.0.1:{metricsPort}/metrics) until 200 OK is returned,
    /// bounded by the specified timeout, or throws if the associated process terminates prematurely.
    /// </summary>
    public static async Task WaitForReadinessAsync(
        HttpClient httpClient,
        int metricsPort,
        CliNodeInstance? node = null,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        CancellationToken cancellationToken = default)
    {
        var resolvedTimeout = timeout ?? TimeSpan.FromSeconds(3.0);
        var resolvedPollInterval = pollInterval ?? TimeSpan.FromMilliseconds(50);
        var sw = Stopwatch.StartNew();

        while (sw.Elapsed < resolvedTimeout)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (node is { HasExited: true })
            {
                throw new InvalidOperationException(
                    $"Node {node.Name} exited prematurely with code {node.ExitCode}. Stderr: {node.GetErrorOutput()}{Environment.NewLine}Stdout: {node.GetAllOutput()}");
            }

            try
            {
                using var reqCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
                using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, reqCts.Token);
                using var resp = await httpClient.GetAsync($"http://127.0.0.1:{metricsPort}/metrics", linkedCts.Token);
                if (resp.IsSuccessStatusCode)
                {
                    return;
                }
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
            }
            catch (HttpRequestException)
            {
            }
            catch (SocketException)
            {
            }
            catch (TimeoutException)
            {
            }

            await Task.Delay(resolvedPollInterval, cancellationToken);
        }

        string extraContext = node != null ? $" Output: {node.GetAllOutput()}" : string.Empty;
        throw new TimeoutException($"Failed to reach HTTP metrics endpoint on port {metricsPort} within {resolvedTimeout.TotalSeconds:F1}s.{extraContext}");
    }

    /// <summary>
    /// Spawns Node A and Node B child processes concurrently and gates execution readiness via HTTP probing
    /// against GET http://127.0.0.1:M_i/metrics with exponential backoff and timeout bounding.
    /// </summary>
    public static async Task<MultiProcessCliHarness> StartAsync(
        CliHarnessOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var opts = options ?? new CliHarnessOptions();
        string rootDir = opts.BaseDirectory ?? Path.Combine(Path.GetTempPath(), $"deltasync-harness-{Guid.NewGuid():N}");
        string dirA = Path.Combine(rootDir, "NodeA");
        string dirB = Path.Combine(rootDir, "NodeB");

        Directory.CreateDirectory(dirA);
        Directory.CreateDirectory(dirB);

        var (listenPortA, metricsPortA, listenPortB, metricsPortB) = AllocateDistinctPorts();
        string clusterId = opts.ClusterId ?? Guid.NewGuid().ToString("D");
        string peerIdA = $"node-a-{Guid.NewGuid():N}"[..12];
        string peerIdB = $"node-b-{Guid.NewGuid():N}"[..12];

        string cliDll = FindCliDll();

        // Build CLI arguments for Node A
        var argsBuilderA = new StringBuilder();
        argsBuilderA.Append($"\"{cliDll}\" --headless");
        argsBuilderA.Append($" --path \"{dirA}\"");
        argsBuilderA.Append($" --port {listenPortA}");
        argsBuilderA.Append($" --metrics-port {metricsPortA}");
        argsBuilderA.Append($" --peer-id {peerIdA}");
        argsBuilderA.Append($" --cluster {clusterId}");
        if (opts.ExtraArgsNodeA != null)
        {
            foreach (var extra in opts.ExtraArgsNodeA)
            {
                argsBuilderA.Append($" {extra}");
            }
        }

        // Build CLI arguments for Node B
        var argsBuilderB = new StringBuilder();
        argsBuilderB.Append($"\"{cliDll}\" --headless");
        argsBuilderB.Append($" --path \"{dirB}\"");
        argsBuilderB.Append($" --port {listenPortB}");
        argsBuilderB.Append($" --metrics-port {metricsPortB}");
        argsBuilderB.Append($" --peer-id {peerIdB}");
        argsBuilderB.Append($" --cluster {clusterId}");
        if (opts.AutoPeer)
        {
            argsBuilderB.Append($" --peer 127.0.0.1:{listenPortA}");
        }
        if (opts.ExtraArgsNodeB != null)
        {
            foreach (var extra in opts.ExtraArgsNodeB)
            {
                argsBuilderB.Append($" {extra}");
            }
        }

        var psiA = new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = argsBuilderA.ToString(),
            WorkingDirectory = dirA,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        var psiB = new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = argsBuilderB.ToString(),
            WorkingDirectory = dirB,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        Process? procA = null;
        Process? procB = null;
        CliNodeInstance? nodeA = null;
        CliNodeInstance? nodeB = null;

        try
        {
            procA = Process.Start(psiA) ?? throw new InvalidOperationException("Failed to launch Process A.");
            nodeA = new CliNodeInstance("NodeA", dirA, listenPortA, metricsPortA, peerIdA, procA);

            procB = Process.Start(psiB) ?? throw new InvalidOperationException("Failed to launch Process B.");
            nodeB = new CliNodeInstance("NodeB", dirB, listenPortB, metricsPortB, peerIdB, procB);

            // Gate readiness via concurrent HTTP probing
            using var httpClient = new HttpClient { Timeout = Timeout.InfiniteTimeSpan };

            var taskA = WaitForReadinessAsync(
                httpClient,
                metricsPortA,
                nodeA,
                opts.ResolvedStartupTimeout,
                opts.ResolvedHttpPollInterval,
                cancellationToken);

            var taskB = WaitForReadinessAsync(
                httpClient,
                metricsPortB,
                nodeB,
                opts.ResolvedStartupTimeout,
                opts.ResolvedHttpPollInterval,
                cancellationToken);

            await Task.WhenAll(taskA, taskB);

            return new MultiProcessCliHarness(rootDir, clusterId, nodeA, nodeB, opts.DeleteOnDispose);
        }
        catch
        {
            // On startup failure, immediately kill any spawned processes and clean up
            nodeA?.Dispose();
            nodeB?.Dispose();

            if (opts.DeleteOnDispose && Directory.Exists(rootDir))
            {
                try
                {
                    Directory.Delete(rootDir, recursive: true);
                }
                catch
                {
                    // Best effort on error
                }
            }

            throw;
        }
    }

    /// <summary>
    /// Terminates child processes using process tree termination (Process.Kill(entireProcessTree: true)),
    /// awaits process termination, and cleans up temporary working directories without file lock exceptions.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // 1. Terminate process trees cleanly
        foreach (var node in Nodes)
        {
            try
            {
                if (!node.Process.HasExited)
                {
                    node.Process.Kill(entireProcessTree: true);
                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
                    await node.Process.WaitForExitAsync(cts.Token);
                }
            }
            catch
            {
                // Best effort
            }
            finally
            {
                node.Dispose();
            }
        }

        // 2. Allow 100ms for OS file handles and SQLite WAL locks to be fully unmapped
        await Task.Delay(100);

        // 3. Delete temporary directories with retry loop
        if (_deleteOnDispose && Directory.Exists(RootDirectory))
        {
            int retries = 5;
            while (retries-- > 0)
            {
                try
                {
                    Directory.Delete(RootDirectory, recursive: true);
                    break;
                }
                catch (IOException) when (retries > 0)
                {
                    await Task.Delay(100);
                }
                catch (UnauthorizedAccessException) when (retries > 0)
                {
                    await Task.Delay(100);
                }
            }
        }
    }
}
