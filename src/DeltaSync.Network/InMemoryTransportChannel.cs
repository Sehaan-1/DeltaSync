namespace DeltaSync.Network;

using System.Net;
using System.Threading.Channels;

/// <summary>
/// In-memory bidirectional transport channel using System.Threading.Channels for zero-copy,
/// high-throughput test simulation and deterministic network fault injection.
/// </summary>
public sealed class InMemoryTransportChannel : IPeerTransportChannel
{
    private readonly Channel<ReadOnlyMemory<byte>> _inbound;
    private readonly ChannelWriter<ReadOnlyMemory<byte>> _outboundWriter;
    private readonly Action _onClose;

    private int _isDisposed;
    private int _isConnected = 1;

    public string LocalPeerId { get; }
    public string RemotePeerId { get; }
    public Guid ClusterId { get; }
    public IPEndPoint? RemoteEndPoint { get; }
    public bool IsInbound { get; }

    public bool IsConnected => Volatile.Read(ref _isConnected) == 1 && !_inbound.Reader.Completion.IsCompleted;
    public bool IsDisposed => Volatile.Read(ref _isDisposed) == 1;

    public string? CloseReason { get; private set; }

    private InMemoryTransportChannel(
        string localPeerId,
        string remotePeerId,
        Guid clusterId,
        IPEndPoint? remoteEndPoint,
        bool isInbound,
        Channel<ReadOnlyMemory<byte>> inbound,
        ChannelWriter<ReadOnlyMemory<byte>> outboundWriter,
        Action onClose)
    {
        LocalPeerId = localPeerId;
        RemotePeerId = remotePeerId;
        ClusterId = clusterId;
        RemoteEndPoint = remoteEndPoint;
        IsInbound = isInbound;
        _inbound = inbound;
        _outboundWriter = outboundWriter;
        _onClose = onClose;
    }

    /// <summary>
    /// Creates a connected pair of in-memory duplex transport channels cross-wired to each other.
    /// What is sent on <paramref name="localPeerId"/> is received on <paramref name="remotePeerId"/>, and vice-versa.
    /// </summary>
    public static (InMemoryTransportChannel Local, InMemoryTransportChannel Remote) CreateConnectedPair(
        string localPeerId,
        string remotePeerId,
        Guid clusterId,
        IPEndPoint? localEndPoint = null,
        IPEndPoint? remoteEndPoint = null,
        bool localIsInbound = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localPeerId);
        ArgumentException.ThrowIfNullOrWhiteSpace(remotePeerId);

        var localInbound = Channel.CreateUnbounded<ReadOnlyMemory<byte>>(new UnboundedChannelOptions
        {
            SingleWriter = false,
            SingleReader = false
        });

        var remoteInbound = Channel.CreateUnbounded<ReadOnlyMemory<byte>>(new UnboundedChannelOptions
        {
            SingleWriter = false,
            SingleReader = false
        });

        var localChannel = new InMemoryTransportChannel(
            localPeerId,
            remotePeerId,
            clusterId,
            remoteEndPoint,
            localIsInbound,
            localInbound,
            remoteInbound.Writer,
            () => remoteInbound.Writer.TryComplete());

        var remoteChannel = new InMemoryTransportChannel(
            remotePeerId,
            localPeerId,
            clusterId,
            localEndPoint,
            !localIsInbound,
            remoteInbound,
            localInbound.Writer,
            () => localInbound.Writer.TryComplete());

        return (localChannel, remoteChannel);
    }

    public async ValueTask SendAsync(ReadOnlyMemory<byte> message, CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);
        if (!IsConnected)
        {
            throw new InvalidOperationException($"Cannot send on closed transport channel to {RemotePeerId}.");
        }

        try
        {
            await _outboundWriter.WriteAsync(message, ct).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            Volatile.Write(ref _isConnected, 0);
            throw new InvalidOperationException($"Remote peer {RemotePeerId} closed the channel.");
        }
    }

    public async ValueTask<ReadOnlyMemory<byte>> ReceiveAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(IsDisposed, this);

        try
        {
            return await _inbound.Reader.ReadAsync(ct).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            Volatile.Write(ref _isConnected, 0);
            return ReadOnlyMemory<byte>.Empty;
        }
    }

    public Task CloseAsync(string reason)
    {
        if (Interlocked.Exchange(ref _isConnected, 0) == 1)
        {
            CloseReason = reason;
            _onClose();
            _inbound.Writer.TryComplete();
        }

        return Task.CompletedTask;
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) == 0)
        {
            Volatile.Write(ref _isConnected, 0);
            _onClose();
            _inbound.Writer.TryComplete();
        }
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
