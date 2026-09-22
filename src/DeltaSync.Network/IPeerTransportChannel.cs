namespace DeltaSync.Network;

using System.Net;

/// <summary>
/// Represents an established bidirectional transport channel between two DeltaSync peers (Spec §3 Step 5).
/// Can be backed by an in-memory duplex channel, a TCP socket stream, or a gRPC duplex stream.
/// </summary>
public interface IPeerTransportChannel : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Canonical identifier of the local peer endpoint.
    /// </summary>
    string LocalPeerId { get; }

    /// <summary>
    /// Canonical identifier of the remote peer endpoint.
    /// </summary>
    string RemotePeerId { get; }

    /// <summary>
    /// Folder synchronization cluster UUID.
    /// </summary>
    Guid ClusterId { get; }

    /// <summary>
    /// Remote network endpoint, or null if using virtual/in-memory transport.
    /// </summary>
    IPEndPoint? RemoteEndPoint { get; }

    /// <summary>
    /// True if this channel was initiated by remote peer dialing into local; false if dialed outbound by local.
    /// </summary>
    bool IsInbound { get; }

    /// <summary>
    /// True if the channel is currently open and healthy.
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    /// True if the underlying transport resources have been released/disposed.
    /// </summary>
    bool IsDisposed { get; }

    /// <summary>
    /// Sends a discrete binary message frame across the duplex channel.
    /// </summary>
    ValueTask SendAsync(ReadOnlyMemory<byte> message, CancellationToken ct = default);

    /// <summary>
    /// Asynchronously receives the next binary message frame from the duplex channel.
    /// Returns empty memory when the remote peer gracefully closes the channel.
    /// </summary>
    ValueTask<ReadOnlyMemory<byte>> ReceiveAsync(CancellationToken ct = default);

    /// <summary>
    /// Closes the channel with a specified reason string.
    /// </summary>
    Task CloseAsync(string reason);
}
