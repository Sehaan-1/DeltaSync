namespace DeltaSync.Network;

/// <summary>
/// Alias for <see cref="PeerConnectionCoordinator"/> to support both architectural naming conventions.
/// </summary>
public sealed class PeerConnectionManager : PeerConnectionCoordinator
{
    public PeerConnectionManager(
        Guid clusterId,
        string localPeerId,
        int listenPort = 0,
        PeerRegistry? registry = null,
        PeerChannelDialer? dialer = null)
        : base(clusterId, localPeerId, listenPort, registry, dialer)
    {
    }
}
