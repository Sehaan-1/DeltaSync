namespace DeltaSync.Network;

using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Text;

/// <summary>
/// Response payload for peer introductory handshake negotiation (Spec §3 Step 5).
/// </summary>
public sealed record HandshakeResponse(
    HandshakeStatus Status,
    string? ErrorMessage = null,
    Guid? ClusterId = null,
    string? PeerId = null,
    ushort ProtocolVersion = HandshakeRequest.CurrentVersion)
{
    /// <summary>
    /// Magic identifier for DeltaSync Handshake Response: 0x44534852 ("DSHR" in ASCII).
    /// </summary>
    public const uint ExpectedMagic = 0x44534852;

    /// <summary>
    /// Fixed header length before variable-length strings:
    /// Magic(4) + Version(2) + Status(2) + ClusterId(16) + PeerIdLen(2) + ErrorMsgLen(2) = 28 bytes.
    /// </summary>
    public const int HeaderSize = 28;

    public bool IsSuccess => Status == HandshakeStatus.Success;

    public static HandshakeResponse CreateSuccess(Guid clusterId, string peerId, ushort protocolVersion = HandshakeRequest.CurrentVersion) =>
        new(HandshakeStatus.Success, null, clusterId, peerId, protocolVersion);

    public static HandshakeResponse CreateClusterMismatch(Guid expectedClusterId, string localPeerId) =>
        new(HandshakeStatus.ClusterMismatch, "Cluster identifier mismatch: folder clusters are isolated (P2).", expectedClusterId, localPeerId);

    public static HandshakeResponse CreateVersionIncompatible(ushort localVersion, string localPeerId) =>
        new(HandshakeStatus.VersionIncompatible, $"Incompatible protocol version. Expected {localVersion}.", null, localPeerId, localVersion);

    public static HandshakeResponse CreateCollisionRejected(string localPeerId) =>
        new(HandshakeStatus.CollisionRejected, "Connection collision rejected: remote node preserved outbound dial under RFC 4271 §6.8 tie-break.", null, localPeerId);

    public static HandshakeResponse CreateDuplicate(string localPeerId) =>
        new(HandshakeStatus.DuplicateConnection, "Duplicate connection rejected: channel already active (I1).", null, localPeerId);

    public static HandshakeResponse CreateMalformed(string error, string? localPeerId = null) =>
        new(HandshakeStatus.MalformedRequest, error, null, localPeerId);

    public int WireSize =>
        HeaderSize +
        (PeerId is not null ? Encoding.UTF8.GetByteCount(PeerId) : 0) +
        (ErrorMessage is not null ? Encoding.UTF8.GetByteCount(ErrorMessage) : 0);

    public int WriteTo(Span<byte> destination)
    {
        int peerIdBytes = PeerId is not null ? Encoding.UTF8.GetByteCount(PeerId) : 0;
        int errorBytes = ErrorMessage is not null ? Encoding.UTF8.GetByteCount(ErrorMessage) : 0;
        int totalSize = HeaderSize + peerIdBytes + errorBytes;

        if (destination.Length < totalSize)
        {
            throw new ArgumentException($"Destination buffer length ({destination.Length}) is too small. Required: {totalSize}.", nameof(destination));
        }

        BinaryPrimitives.WriteUInt32LittleEndian(destination[..4], ExpectedMagic);
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(4, 2), ProtocolVersion);
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(6, 2), (ushort)Status);

        Span<byte> clusterGuidSpan = stackalloc byte[16];
        (ClusterId ?? Guid.Empty).TryWriteBytes(clusterGuidSpan);
        clusterGuidSpan.CopyTo(destination.Slice(8, 16));

        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(24, 2), (ushort)peerIdBytes);
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(26, 2), (ushort)errorBytes);

        int offset = HeaderSize;
        if (peerIdBytes > 0)
        {
            Encoding.UTF8.GetBytes(PeerId!, destination.Slice(offset, peerIdBytes));
            offset += peerIdBytes;
        }

        if (errorBytes > 0)
        {
            Encoding.UTF8.GetBytes(ErrorMessage!, destination.Slice(offset, errorBytes));
        }

        return totalSize;
    }

    public byte[] ToByteArray()
    {
        byte[] buffer = new byte[WireSize];
        WriteTo(buffer);
        return buffer;
    }

    public static bool TryParse(ReadOnlySpan<byte> source, [NotNullWhen(true)] out HandshakeResponse? response, [NotNullWhen(false)] out string? error)
    {
        response = null;

        if (source.Length < HeaderSize)
        {
            error = $"Truncated handshake response: length {source.Length} is less than header size {HeaderSize}.";
            return false;
        }

        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(source[..4]);
        if (magic != ExpectedMagic)
        {
            error = $"Invalid handshake response magic 0x{magic:X8}. Expected 0x{ExpectedMagic:X8}.";
            return false;
        }

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(4, 2));
        ushort rawStatus = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(6, 2));
        Guid clusterId = new(source.Slice(8, 16));
        ushort peerIdLen = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(24, 2));
        ushort errorLen = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(26, 2));

        if (source.Length < HeaderSize + peerIdLen + errorLen)
        {
            error = $"Truncated body in handshake response: expected {HeaderSize + peerIdLen + errorLen} bytes, got {source.Length}.";
            return false;
        }

        string? peerId = peerIdLen > 0
            ? Encoding.UTF8.GetString(source.Slice(HeaderSize, peerIdLen))
            : null;

        string? errorMessage = errorLen > 0
            ? Encoding.UTF8.GetString(source.Slice(HeaderSize + peerIdLen, errorLen))
            : null;

        var status = Enum.IsDefined(typeof(HandshakeStatus), (int)rawStatus)
            ? (HandshakeStatus)rawStatus
            : HandshakeStatus.MalformedRequest;

        response = new HandshakeResponse(
            status,
            errorMessage,
            clusterId == Guid.Empty ? null : clusterId,
            peerId,
            version);

        error = null;
        return true;
    }
}
