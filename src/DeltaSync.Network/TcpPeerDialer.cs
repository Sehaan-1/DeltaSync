namespace DeltaSync.Network;

using System.Net;
using System.Net.Sockets;

/// <summary>
/// High-performance outbound dialer for establishing TCP peer transport channels (Spec §3 Step 4).
/// Conforms to the <see cref="PeerChannelDialer"/> delegate.
/// </summary>
public sealed class TcpPeerDialer
{
    public static readonly TimeSpan DefaultDialTimeout = TimeSpan.FromSeconds(5.0);

    public Guid ClusterId { get; }
    public string LocalPeerId { get; }
    public TimeSpan DialTimeout { get; }
    public int InboundQueueCapacity { get; }
    public PeerRegistry? Registry { get; }

    public TcpPeerDialer(
        Guid clusterId,
        string localPeerId,
        PeerRegistry? registry = null,
        TimeSpan? dialTimeout = null,
        int inboundQueueCapacity = TcpTransportChannel.DefaultInboundQueueCapacity)
    {
        if (clusterId == Guid.Empty)
            throw new ArgumentException("ClusterId cannot be empty.", nameof(clusterId));
        ArgumentException.ThrowIfNullOrWhiteSpace(localPeerId);

        ClusterId = clusterId;
        LocalPeerId = localPeerId;
        Registry = registry;
        DialTimeout = dialTimeout ?? DefaultDialTimeout;
        InboundQueueCapacity = inboundQueueCapacity;
    }

    /// <summary>
    /// Connects to a remote peer endpoint over TCP with strict timeout and TCP_NODELAY.
    /// Wraps the connected stream into an outbound <see cref="TcpTransportChannel"/>.
    /// </summary>
    public async ValueTask<IPeerTransportChannel> DialAsync(
        string remotePeerId,
        IPEndPoint? endpoint = null,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(remotePeerId);

        endpoint ??= Registry is not null && Registry.TryGetPeer(remotePeerId, out var peer) ? peer.Endpoint : null;
        if (endpoint is null)
        {
            throw new ArgumentException($"Remote endpoint cannot be null and was not found in registry for peer '{remotePeerId}'.", nameof(endpoint));
        }

        var tcpClient = new TcpClient();
        try
        {
            try
            {
                tcpClient.NoDelay = true;
            }
            catch
            {
                // Best effort before connection
            }

            using var timeoutCts = new CancellationTokenSource(DialTimeout);
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);

            try
            {
                await tcpClient.ConnectAsync(endpoint.Address, endpoint.Port, linkedCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
            {
                throw new TimeoutException($"TCP connection attempt to peer '{remotePeerId}' at {endpoint} timed out after {DialTimeout.TotalSeconds:F1}s.");
            }

            try
            {
                tcpClient.NoDelay = true;
            }
            catch
            {
            }

            Stream stream = tcpClient.GetStream();

            return new TcpTransportChannel(
                localPeerId: LocalPeerId,
                remotePeerId: remotePeerId,
                clusterId: ClusterId,
                tcpClient: tcpClient,
                stream: stream,
                isInbound: false,
                remoteEndPoint: endpoint,
                inboundQueueCapacity: InboundQueueCapacity);
        }
        catch
        {
            tcpClient.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Returns a delegate compatible with <see cref="PeerChannelDialer"/>.
    /// </summary>
    public PeerChannelDialer AsDialer() => DialAsync;

    /// <summary>
    /// Implicit conversion to <see cref="PeerChannelDialer"/> delegate.
    /// </summary>
    public static implicit operator PeerChannelDialer(TcpPeerDialer dialer) => dialer.DialAsync;
}
