namespace DeltaSync.Network;

public record PeerInfo(string PeerId, string Endpoint, bool IsConnected);
