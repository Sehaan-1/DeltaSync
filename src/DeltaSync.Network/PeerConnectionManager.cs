namespace DeltaSync.Network;

using System.Diagnostics.CodeAnalysis;
using System.Net;

/// <summary>
/// Alias and wrapper for <see cref="PeerConnectionCoordinator"/> to support both architectural naming conventions.
/// </summary>
public sealed class PeerConnectionManager : IPeerConnectionCoordinator
{
    private readonly PeerConnectionCoordinator _inner;

    public PeerConnectionManager(
        Guid clusterId,
        string localPeerId,
        int listenPort = 0,
        PeerRegistry? registry = null,
        PeerChannelDialer? dialer = null)
    {
        _inner = new PeerConnectionCoordinator(clusterId, localPeerId, listenPort, registry, dialer);
    }

    public Guid ClusterId => _inner.ClusterId;
    public string LocalPeerId => _inner.LocalPeerId;
    public int ListenPort => _inner.ListenPort;
    public PeerRegistry Registry => _inner.Registry;

    public IReadOnlyCollection<IPeerTransportChannel> ActiveConnections => _inner.ActiveConnections;

    public event EventHandler<IPeerTransportChannel>? ConnectionEstablished
    {
        add => _inner.ConnectionEstablished += value;
        remove => _inner.ConnectionEstablished -= value;
    }

    public event EventHandler<string>? ConnectionClosed
    {
        add => _inner.ConnectionClosed += value;
        remove => _inner.ConnectionClosed -= value;
    }

    public event EventHandler<(string RemotePeerId, CollisionDecision Decision)>? CollisionResolved
    {
        add => _inner.CollisionResolved += value;
        remove => _inner.CollisionResolved -= value;
    }

    public bool TryGetConnection(string peerId, [NotNullWhen(true)] out IPeerTransportChannel? channel)
        => _inner.TryGetConnection(peerId, out channel);

    public Task<IPeerTransportChannel?> ConnectAsync(string remotePeerId, IPEndPoint? endpoint = null, CancellationToken ct = default)
        => _inner.ConnectAsync(remotePeerId, endpoint, ct);

    public Task<bool> AcceptConnectionAsync(IPeerTransportChannel channel, CancellationToken ct = default)
        => _inner.AcceptConnectionAsync(channel, ct);

    public Task CloseConnectionAsync(string peerId, string reason = "Normal closure")
        => _inner.CloseConnectionAsync(peerId, reason);

    public void Dispose() => _inner.Dispose();
    public ValueTask DisposeAsync() => _inner.DisposeAsync();
}
