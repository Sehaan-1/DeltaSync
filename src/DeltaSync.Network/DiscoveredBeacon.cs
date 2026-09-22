namespace DeltaSync.Network;

using System.Net;

/// <summary>
/// Represents a validated discovery beacon received from a remote peer node.
/// </summary>
public sealed record DiscoveredBeacon(BeaconFrame Frame, IPEndPoint RemoteEndPoint)
{
    /// <summary>
    /// The gRPC/TCP synchronization service endpoint of the remote peer.
    /// Combines the remote sender's IP address with the declared ListenPort in the beacon.
    /// </summary>
    public IPEndPoint ServiceEndPoint => new(RemoteEndPoint.Address, Frame.ListenPort);
}
