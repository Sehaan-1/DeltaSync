using System.Net;
using System.Net.Sockets;
using System.Text;

namespace DeltaSync.Core.Metrics;

/// <summary>
/// Lightweight local HTTP server serving Prometheus text exposition metrics (ADR-0005).
/// </summary>
public sealed class PrometheusMetricsServer : IAsyncDisposable, IDisposable
{
    private readonly SyncMetricsSink _sink;
    private readonly HttpListener _listener;
    private readonly CancellationTokenSource _cts = new();
    private Task? _listenTask;
    private bool _isDisposed;

    /// <summary>
    /// The port on which the HTTP server is listening.
    /// </summary>
    public int Port { get; }

    /// <summary>
    /// The host address bound to the listener (default: 127.0.0.1).
    /// </summary>
    public string Host { get; }

    /// <summary>
    /// Full endpoint URI for the metrics scrape path.
    /// </summary>
    public Uri MetricsUri => new($"http://{Host}:{Port}/metrics");

    public PrometheusMetricsServer(SyncMetricsSink sink, int port = 9090, string host = "127.0.0.1")
    {
        _sink = sink ?? throw new ArgumentNullException(nameof(sink));
        Port = port;
        Host = string.IsNullOrWhiteSpace(host) ? "127.0.0.1" : host;

        _listener = new HttpListener();
        _listener.Prefixes.Add($"http://{Host}:{Port}/");
        if (Host == "127.0.0.1")
        {
            try
            {
                _listener.Prefixes.Add($"http://localhost:{Port}/");
            }
            catch
            {
                // Fall back to 127.0.0.1 only if localhost prefix reservation fails
            }
        }
    }

    /// <summary>
    /// Starts listening for incoming HTTP requests on a background task.
    /// </summary>
    public void Start()
    {
        ObjectDisposedException.ThrowIf(_isDisposed, this);
        if (_listener.IsListening) return;

        _listener.Start();
        _listenTask = Task.Run(ListenLoopAsync);
    }

    private async Task ListenLoopAsync()
    {
        while (!_cts.IsCancellationRequested && _listener.IsListening)
        {
            try
            {
                var context = await _listener.GetContextAsync().ConfigureAwait(false);
                _ = Task.Run(() => HandleContextAsync(context));
            }
            catch (HttpListenerException) when (_cts.IsCancellationRequested || !_listener.IsListening)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (Exception) when (!_listener.IsListening)
            {
                break;
            }
        }
    }

    private async Task HandleContextAsync(HttpListenerContext context)
    {
        try
        {
            string path = context.Request.Url?.AbsolutePath ?? "/";
            if (string.Equals(path, "/metrics", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(path, "/metrics/", StringComparison.OrdinalIgnoreCase))
            {
                string payload = PrometheusExporter.Export(_sink);
                byte[] bytes = Encoding.UTF8.GetBytes(payload);

                context.Response.StatusCode = (int)HttpStatusCode.OK;
                context.Response.ContentType = PrometheusExporter.ContentType;
                context.Response.ContentLength64 = bytes.Length;
                await context.Response.OutputStream.WriteAsync(bytes, _cts.Token).ConfigureAwait(false);
            }
            else
            {
                context.Response.StatusCode = (int)HttpStatusCode.NotFound;
            }
        }
        catch
        {
            // Suppress IO and client disconnect exceptions during response streaming
        }
        finally
        {
            try
            {
                context.Response.Close();
            }
            catch
            {
                // Suppress disposal races
            }
        }
    }

    /// <summary>
    /// Discovers an available ephemeral loopback TCP port for testing.
    /// </summary>
    public static int GetAvailablePort()
    {
        using var tcp = new TcpListener(IPAddress.Loopback, 0);
        tcp.Start();
        int port = ((IPEndPoint)tcp.LocalEndpoint).Port;
        tcp.Stop();
        return port;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_isDisposed) return;
        _isDisposed = true;

        _cts.Cancel();
        try
        {
            if (_listener.IsListening)
            {
                _listener.Stop();
            }
            _listener.Close();
        }
        catch
        {
            // Suppress stop errors
        }

        _cts.Dispose();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        Dispose();
        if (_listenTask != null)
        {
            try
            {
                await _listenTask.ConfigureAwait(false);
            }
            catch
            {
                // Suppress shutdown cancellation
            }
        }
    }
}
