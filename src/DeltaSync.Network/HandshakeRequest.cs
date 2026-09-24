namespace DeltaSync.Network;

using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Text;

/// <summary>
/// Introductory handshake request transmitted when establishing a duplex peer transport channel (Spec §3 Step 5).
/// </summary>
public sealed record HandshakeRequest(
    Guid ClusterId,
    string PeerId,
    ushort ProtocolVersion = HandshakeRequest.CurrentVersion,
    int ListenPort = 0)
{
    /// <summary>
    /// Magic identifier for DeltaSync Handshake: 0x4453484B ("DSHK" in ASCII).
    /// </summary>
    public const uint ExpectedMagic = 0x4453484B;

    /// <summary>
    /// Current supported handshake protocol version.
    /// </summary>
    public const ushort CurrentVersion = 1;

    /// <summary>
    /// Oldest protocol version this node will accept from a remote peer.
    /// Increment this only on breaking wire-format changes.
    /// </summary>
    public const ushort MinVersion = 1;

    /// <summary>
    /// Maximum allowed UTF-8 byte length for canonical PeerId (Spec §2 Preconditions: &lt;= 64 bytes).
    /// </summary>
    public const int MaxPeerIdByteLength = 64;

    /// <summary>
    /// Fixed header length before variable-length PeerId: Magic(4) + Version(2) + ClusterId(16) + ListenPort(4) + PeerIdLen(2) = 28 bytes.
    /// </summary>
    public const int HeaderSize = 28;

    /// <summary>
    /// Validates handshake request preconditions.
    /// </summary>
    public bool IsValid([NotNullWhen(false)] out string? error)
    {
        if (ClusterId == Guid.Empty)
        {
            error = "ClusterId cannot be empty.";
            return false;
        }

        if (string.IsNullOrWhiteSpace(PeerId))
        {
            error = "PeerId cannot be null or whitespace.";
            return false;
        }

        int byteCount = Encoding.UTF8.GetByteCount(PeerId);
        if (byteCount > MaxPeerIdByteLength)
        {
            error = $"PeerId UTF-8 byte length ({byteCount}) exceeds maximum allowed ({MaxPeerIdByteLength}).";
            return false;
        }

        if (ProtocolVersion == 0)
        {
            error = "ProtocolVersion must be greater than zero.";
            return false;
        }

        if (ListenPort is < 0 or > 65535)
        {
            error = $"ListenPort ({ListenPort}) is outside valid TCP port range [0..65535].";
            return false;
        }

        error = null;
        return true;
    }

    /// <summary>
    /// Computes the exact serialized wire size for this request.
    /// </summary>
    public int WireSize => HeaderSize + Encoding.UTF8.GetByteCount(PeerId);

    /// <summary>
    /// Serializes this request into the destination buffer.
    /// </summary>
    public int WriteTo(Span<byte> destination)
    {
        int peerIdBytes = Encoding.UTF8.GetByteCount(PeerId);
        int totalSize = HeaderSize + peerIdBytes;

        if (destination.Length < totalSize)
        {
            throw new ArgumentException($"Destination buffer length ({destination.Length}) is too small. Required: {totalSize}.", nameof(destination));
        }

        BinaryPrimitives.WriteUInt32LittleEndian(destination[..4], ExpectedMagic);
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(4, 2), ProtocolVersion);

        Span<byte> clusterGuidSpan = stackalloc byte[16];
        if (!ClusterId.TryWriteBytes(clusterGuidSpan))
        {
            throw new InvalidOperationException("Failed to format ClusterId GUID.");
        }
        clusterGuidSpan.CopyTo(destination.Slice(6, 16));

        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(22, 4), ListenPort);
        BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(26, 2), (ushort)peerIdBytes);

        Encoding.UTF8.GetBytes(PeerId, destination.Slice(HeaderSize, peerIdBytes));
        return totalSize;
    }

    /// <summary>
    /// Serializes this request into a newly allocated byte array.
    /// </summary>
    public byte[] ToByteArray()
    {
        byte[] buffer = new byte[WireSize];
        WriteTo(buffer);
        return buffer;
    }

    /// <summary>
    /// Attempts to parse a binary handshake request frame from the provided span.
    /// </summary>
    public static bool TryParse(ReadOnlySpan<byte> source, [NotNullWhen(true)] out HandshakeRequest? request, [NotNullWhen(false)] out string? error)
    {
        request = null;

        if (source.Length < HeaderSize)
        {
            error = $"Truncated handshake request: length {source.Length} is less than header size {HeaderSize}.";
            return false;
        }

        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(source[..4]);
        if (magic != ExpectedMagic)
        {
            error = $"Invalid handshake magic 0x{magic:X8}. Expected 0x{ExpectedMagic:X8}.";
            return false;
        }

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(4, 2));
        Guid clusterId = new(source.Slice(6, 16));
        int listenPort = BinaryPrimitives.ReadInt32LittleEndian(source.Slice(22, 4));
        ushort peerIdLen = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(26, 2));

        if (source.Length < HeaderSize + peerIdLen)
        {
            error = $"Truncated PeerId in handshake request: expected {peerIdLen} bytes, got {source.Length - HeaderSize}.";
            return false;
        }

        if (peerIdLen > MaxPeerIdByteLength)
        {
            error = $"PeerId byte length {peerIdLen} exceeds maximum limit of {MaxPeerIdByteLength}.";
            return false;
        }

        string peerId = Encoding.UTF8.GetString(source.Slice(HeaderSize, peerIdLen));
        var candidate = new HandshakeRequest(clusterId, peerId, version, listenPort);

        if (!candidate.IsValid(out error))
        {
            return false;
        }

        request = candidate;
        error = null;
        return true;
    }
}
