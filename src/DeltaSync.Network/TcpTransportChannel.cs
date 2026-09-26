namespace DeltaSync.Network;

using System.Buffers.Binary;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;

/// <summary>
/// High-performance bidirectional transport channel over streaming TCP sockets or TLS streams (Spec §3 Steps 1–2).
/// Demarcates streams with 4-byte big-endian length prefix headers and enforces bounded backpressure.
/// </summary>
public sealed class TcpTransportChannel : IPeerTransportChannel
{
    public const int LengthHeaderSize = 4;
    public const int MaxFrameSize = 4 * 1024 * 1024; // 4 MB (matches SyncWireFrameSerializer.MaxFrameSize)
    public const int DefaultInboundQueueCapacity = 32;

    private readonly Stream _stream;
    private readonly TcpClient? _tcpClient;
    private readonly Socket? _socket;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly byte[] _sendHeaderBuffer = new byte[LengthHeaderSize];
    private readonly Channel<ReadOnlyMemory<byte>> _inboundQueue;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _readLoopTask;

    private int _isConnected = 1;
    private int _isDisposed;

    public string LocalPeerId { get; }
    public string RemotePeerId { get; internal set; }
    public Guid ClusterId { get; }
    public IPEndPoint? RemoteEndPoint { get; }
    public bool IsInbound { get; }

    public bool IsConnected => Volatile.Read(ref _isConnected) == 1 && !_inboundQueue.Reader.Completion.IsCompleted;
    public bool IsDisposed => Volatile.Read(ref _isDisposed) == 1;
    public string? CloseReason { get; private set; }

    public TcpTransportChannel(
        string localPeerId,
        string remotePeerId,
        Guid clusterId,
        Stream stream,
        bool isInbound,
        IPEndPoint? remoteEndPoint = null,
        TcpClient? tcpClient = null,
        Socket? socket = null,
        int inboundQueueCapacity = DefaultInboundQueueCapacity)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localPeerId);
        ArgumentNullException.ThrowIfNull(stream);
        if (clusterId == Guid.Empty)
            throw new ArgumentException("ClusterId cannot be empty.", nameof(clusterId));

        LocalPeerId = localPeerId;
        RemotePeerId = remotePeerId ?? string.Empty;
        ClusterId = clusterId;
        IsInbound = isInbound;
        _stream = stream;
        _tcpClient = tcpClient;
        _socket = socket ?? tcpClient?.Client;
        RemoteEndPoint = remoteEndPoint ?? (_socket?.RemoteEndPoint as IPEndPoint);

        if (_socket is not null)
        {
            try
            {
                _socket.NoDelay = true;
            }
            catch
            {
                // In case socket is non-configurable or already disconnected
            }
        }

        var channelOptions = new BoundedChannelOptions(inboundQueueCapacity > 0 ? inboundQueueCapacity : DefaultInboundQueueCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true
        };
        _inboundQueue = Channel.CreateBounded<ReadOnlyMemory<byte>>(channelOptions);

        _readLoopTask = Task.Run(RunReadLoopAsync);
    }

    public TcpTransportChannel(
        string localPeerId,
        string remotePeerId,
        Guid clusterId,
        TcpClient tcpClient,
        Stream stream,
        bool isInbound,
        IPEndPoint? remoteEndPoint = null,
        int inboundQueueCapacity = DefaultInboundQueueCapacity)
        : this(localPeerId, remotePeerId, clusterId, stream, isInbound, remoteEndPoint, tcpClient, tcpClient.Client, inboundQueueCapacity)
    {
    }

    public static async Task<(TcpTransportChannel Local, TcpTransportChannel Remote)> CreateConnectedTcpPairAsync(
        string localPeerId = "local-peer",
        string remotePeerId = "remote-peer",
        Guid? clusterId = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localPeerId);
        ArgumentException.ThrowIfNullOrWhiteSpace(remotePeerId);

        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var client = new TcpClient();
        var connectTask = client.ConnectAsync(IPAddress.Loopback, port);
        var serverClient = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
        await connectTask.ConfigureAwait(false);
        listener.Stop();

        client.NoDelay = true;
        serverClient.NoDelay = true;

        var cid = clusterId ?? Guid.NewGuid();
        var localChannel = new TcpTransportChannel(
            localPeerId,
            remotePeerId,
            cid,
            client.GetStream(),
            isInbound: false,
            remoteEndPoint: (IPEndPoint?)serverClient.Client.LocalEndPoint,
            tcpClient: client,
            socket: client.Client);

        var remoteChannel = new TcpTransportChannel(
            remotePeerId,
            localPeerId,
            cid,
            serverClient.GetStream(),
            isInbound: true,
            remoteEndPoint: (IPEndPoint?)client.Client.LocalEndPoint,
            tcpClient: serverClient,
            socket: serverClient.Client);

        return (localChannel, remoteChannel);
    }

    public async ValueTask SendAsync(ReadOnlyMemory<byte> message, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);
        if (!IsConnected)
        {
            throw new InvalidOperationException($"Cannot send on closed transport channel to {RemotePeerId}.");
        }

        if (message.Length < 1 || message.Length > MaxFrameSize)
        {
            throw new ArgumentOutOfRangeException(nameof(message),
                $"Message length {message.Length} is outside valid frame bounds (1 to {MaxFrameSize} bytes).");
        }

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
        var token = linkedCts.Token;

        await _sendLock.WaitAsync(token).ConfigureAwait(false);
        try
        {
            BinaryPrimitives.WriteUInt32BigEndian(_sendHeaderBuffer, (uint)message.Length);
            await _stream.WriteAsync(_sendHeaderBuffer.AsMemory(), token).ConfigureAwait(false);
            await _stream.WriteAsync(message, token).ConfigureAwait(false);
            await _stream.FlushAsync(token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is IOException or SocketException)
        {
            Volatile.Write(ref _isConnected, 0);
            SafeCloseSocket();
            throw new InvalidOperationException($"Failed to transmit frame to {RemotePeerId}: {ex.Message}", ex);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public async ValueTask<ReadOnlyMemory<byte>> ReceiveAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);

        try
        {
            return await _inboundQueue.Reader.ReadAsync(ct).ConfigureAwait(false);
        }
        catch (ChannelClosedException cce)
        {
            Volatile.Write(ref _isConnected, 0);
            if (cce.InnerException is not null)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(cce.InnerException).Throw();
            }
            return ReadOnlyMemory<byte>.Empty;
        }
    }

    public async Task CloseAsync(string reason)
    {
        if (Interlocked.Exchange(ref _isConnected, 0) == 1)
        {
            CloseReason = reason;
            _cts.Cancel();
            SafeCloseSocket();
            _inboundQueue.Writer.TryComplete();
            try
            {
                await _readLoopTask.ConfigureAwait(false);
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
            Volatile.Write(ref _isConnected, 0);
            _cts.Cancel();
            SafeCloseSocket();
            _inboundQueue.Writer.TryComplete();
            _cts.Dispose();
            _sendLock.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) == 0)
        {
            Volatile.Write(ref _isConnected, 0);
            _cts.Cancel();
            SafeCloseSocket();
            _inboundQueue.Writer.TryComplete();
            try
            {
                await _readLoopTask.ConfigureAwait(false);
            }
            catch
            {
            }
            _cts.Dispose();
            _sendLock.Dispose();
        }
    }

    private async Task RunReadLoopAsync()
    {
        byte[] lengthBuffer = new byte[LengthHeaderSize];
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                // Read first byte to detect clean EOF at frame boundary
                int bytesRead = await _stream.ReadAsync(lengthBuffer.AsMemory(0, 1), _cts.Token).ConfigureAwait(false);
                if (bytesRead == 0)
                {
                    // Clean EOF from remote peer
                    break;
                }

                // Read remaining 3 bytes of length header
                await _stream.ReadExactlyAsync(lengthBuffer.AsMemory(1, LengthHeaderSize - 1), _cts.Token).ConfigureAwait(false);

                uint frameLength = BinaryPrimitives.ReadUInt32BigEndian(lengthBuffer);
                if (frameLength < 1 || frameLength > MaxFrameSize)
                {
                    throw new InvalidDataException(
                        $"Invalid transport frame length: {frameLength} bytes (expected 1 to {MaxFrameSize} bytes).");
                }

                byte[] payload = new byte[frameLength];
                await _stream.ReadExactlyAsync(payload.AsMemory(), _cts.Token).ConfigureAwait(false);

                await _inboundQueue.Writer.WriteAsync(payload, _cts.Token).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (_cts.IsCancellationRequested)
        {
            // Expected on cancellation/closure
        }
        catch (EndOfStreamException)
        {
            // Remote dropped connection mid-frame
            CloseReason ??= "Remote endpoint closed stream unexpectedly mid-frame.";
        }
        catch (Exception ex)
        {
            CloseReason ??= ex.Message;
            Volatile.Write(ref _isConnected, 0);
            SafeCloseSocket();
            _inboundQueue.Writer.TryComplete(ex);
            return;
        }
        finally
        {
            Volatile.Write(ref _isConnected, 0);
            _inboundQueue.Writer.TryComplete();
        }
    }

    private void SafeCloseSocket()
    {
        try
        {
            if (_socket is { Connected: true })
            {
                _socket.Shutdown(SocketShutdown.Both);
            }
        }
        catch
        {
            // Socket already closed or reset
        }

        try
        {
            _stream.Dispose();
        }
        catch
        {
        }

        try
        {
            _tcpClient?.Dispose();
        }
        catch
        {
        }
    }
}
