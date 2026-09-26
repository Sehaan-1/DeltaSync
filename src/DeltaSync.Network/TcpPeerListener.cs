namespace DeltaSync.Network;

using System.Net;
using System.Net.Sockets;

/// <summary>
/// Server endpoint that binds a local TCP port and accepts inbound peer connections (Spec §3 Step 3).
/// Wraps incoming sockets into <see cref="TcpTransportChannel"/> and hands them to <see cref="IPeerConnectionCoordinator.AcceptConnectionAsync"/>.
/// </summary>
public sealed class TcpPeerListener : IAsyncDisposable, IDisposable
{
    private readonly string _localPeerId;
    private readonly Guid _clusterId;
    private readonly IPeerConnectionCoordinator _coordinator;
    private readonly int _inboundQueueCapacity;
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _cts = new();

    private Task? _acceptLoopTask;
    private int _isListening;
    private int _isDisposed;

    public string LocalPeerId => _localPeerId;
    public Guid ClusterId => _clusterId;
    public IPEndPoint LocalEndPoint { get; private set; }
    public int Port => LocalEndPoint.Port;
    public bool IsListening => Volatile.Read(ref _isListening) == 1;

    public TcpPeerListener(
        string localPeerId,
        Guid clusterId,
        IPEndPoint listenEndPoint,
        IPeerConnectionCoordinator coordinator,
        int inboundQueueCapacity = TcpTransportChannel.DefaultInboundQueueCapacity)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localPeerId);
        if (clusterId == Guid.Empty)
            throw new ArgumentException("ClusterId cannot be empty.", nameof(clusterId));
        ArgumentNullException.ThrowIfNull(listenEndPoint);
        ArgumentNullException.ThrowIfNull(coordinator);

        _localPeerId = localPeerId;
        _clusterId = clusterId;
        _coordinator = coordinator;
        _inboundQueueCapacity = inboundQueueCapacity;

        _listener = new TcpListener(listenEndPoint);
        LocalEndPoint = listenEndPoint;
    }

    public TcpPeerListener(
        string localPeerId,
        Guid clusterId,
        int port,
        IPeerConnectionCoordinator coordinator,
        IPAddress? bindAddress = null,
        int inboundQueueCapacity = TcpTransportChannel.DefaultInboundQueueCapacity)
        : this(localPeerId, clusterId, new IPEndPoint(bindAddress ?? IPAddress.Any, port), coordinator, inboundQueueCapacity)
    {
    }

    /// <summary>
    /// Starts the underlying TCP listener and begins accepting inbound peer connections.
    /// </summary>
    public void Start()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed) == 1, this);

        if (Interlocked.Exchange(ref _isListening, 1) == 1)
        {
            return;
        }

        _listener.Start();
        if (_listener.LocalEndpoint is IPEndPoint activeEp)
        {
            LocalEndPoint = activeEp;
        }

        _acceptLoopTask = Task.Run(() => RunAcceptLoopAsync(_cts.Token));
    }

    /// <summary>
    /// Asynchronously stops the listener and terminates the accept loop.
    /// </summary>
    public async Task StopAsync()
    {
        if (Interlocked.Exchange(ref _isListening, 0) == 0)
        {
            return;
        }

        _cts.Cancel();
        try
        {
            _listener.Stop();
        }
        catch
        {
        }

        if (_acceptLoopTask is not null)
        {
            try
            {
                await _acceptLoopTask.ConfigureAwait(false);
            }
            catch
            {
            }
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) == 0)
        {
            Volatile.Write(ref _isListening, 0);
            _cts.Cancel();
            try
            {
                _listener.Stop();
            }
            catch
            {
            }
            _cts.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) == 0)
        {
            await StopAsync().ConfigureAwait(false);
            _cts.Dispose();
        }
    }

    private async Task RunAcceptLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            TcpClient tcpClient;
            try
            {
                tcpClient = await _listener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (SocketException ex) when (ct.IsCancellationRequested ||
                                             ex.SocketErrorCode == SocketError.OperationAborted ||
                                             ex.SocketErrorCode == SocketError.Interrupted)
            {
                break;
            }
            catch (Exception)
            {
                // Transient network / accept failure, continue loop
                continue;
            }

            try
            {
                tcpClient.NoDelay = true;
            }
            catch
            {
            }

            _ = Task.Run(async () =>
            {
                try
                {
                    Stream stream = tcpClient.GetStream();
                    var remoteEndPoint = tcpClient.Client.RemoteEndPoint as IPEndPoint;
                    var channel = new TcpTransportChannel(
                        localPeerId: _localPeerId,
                        remotePeerId: string.Empty,
                        clusterId: _clusterId,
                        tcpClient: tcpClient,
                        stream: stream,
                        isInbound: true,
                        remoteEndPoint: remoteEndPoint,
                        inboundQueueCapacity: _inboundQueueCapacity);

                    await _coordinator.AcceptConnectionAsync(channel, CancellationToken.None).ConfigureAwait(false);
                }
                catch
                {
                    try
                    {
                        tcpClient.Dispose();
                    }
                    catch
                    {
                    }
                }
            }, CancellationToken.None);
        }
    }
}
