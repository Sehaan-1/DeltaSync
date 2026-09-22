namespace DeltaSync.Network;

using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.Security.Cryptography;
using System.Text;

/// <summary>
/// Immutable binary beacon frame transmitted via UDP multicast for local peer discovery.
/// Uses a compact, fixed 72-byte wire layout with zero heap allocation on invalid frames.
/// </summary>
public sealed record BeaconFrame : IEquatable<BeaconFrame>
{
    /// <summary>
    /// Exact binary frame size in bytes.
    /// </summary>
    public const int FrameSize = 72;

    /// <summary>
    /// Magic header identifier: 0x4453594E ("DSYN" in ASCII).
    /// </summary>
    public const uint ExpectedMagic = 0x4453594E;

    /// <summary>
    /// Current binary protocol version.
    /// </summary>
    public const ushort CurrentVersion = 1;

    public uint Magic { get; }
    public ushort ProtocolVersion { get; }
    public Guid ClusterId { get; }
    public byte[] PeerIdHash { get; }
    public ushort ListenPort { get; }
    public ulong Sequence { get; }
    public long TimestampUnixMs { get; }

    /// <summary>
    /// Hexadecimal lowercase string representation of the 32-byte PeerIdHash.
    /// </summary>
    public string PeerIdHashHex => Convert.ToHexString(PeerIdHash).ToLowerInvariant();

    public BeaconFrame(
        uint magic,
        ushort protocolVersion,
        Guid clusterId,
        byte[] peerIdHash,
        ushort listenPort,
        ulong sequence,
        long timestampUnixMs)
    {
        if (magic != ExpectedMagic)
            throw new ArgumentException($"Invalid magic: 0x{magic:X8}. Expected 0x{ExpectedMagic:X8}.", nameof(magic));
        if (protocolVersion != CurrentVersion)
            throw new ArgumentException($"Unsupported protocol version: {protocolVersion}. Expected {CurrentVersion}.", nameof(protocolVersion));
        ArgumentNullException.ThrowIfNull(peerIdHash);
        if (peerIdHash.Length != 32)
            throw new ArgumentException("PeerIdHash must be a 32-byte SHA-256 digest.", nameof(peerIdHash));

        Magic = magic;
        ProtocolVersion = protocolVersion;
        ClusterId = clusterId;
        PeerIdHash = (byte[])peerIdHash.Clone();
        ListenPort = listenPort;
        Sequence = sequence;
        TimestampUnixMs = timestampUnixMs;
    }

    /// <summary>
    /// Computes the canonical 32-byte SHA-256 hash of a UTF-8 peer identifier.
    /// </summary>
    public static byte[] ComputePeerIdHash(string peerId)
    {
        ArgumentNullException.ThrowIfNull(peerId);
        return SHA256.HashData(Encoding.UTF8.GetBytes(peerId));
    }

    /// <summary>
    /// Computes the canonical 32-byte SHA-256 hash of a peer identifier with zero heap allocations.
    /// </summary>
    public static void ComputePeerIdHash(ReadOnlySpan<char> peerId, Span<byte> destination)
    {
        if (destination.Length < 32)
            throw new ArgumentException("Destination span must be at least 32 bytes.", nameof(destination));

        int maxUtf8Bytes = Encoding.UTF8.GetMaxByteCount(peerId.Length);
        Span<byte> utf8Bytes = maxUtf8Bytes <= 256 ? stackalloc byte[maxUtf8Bytes] : new byte[maxUtf8Bytes];
        int written = Encoding.UTF8.GetBytes(peerId, utf8Bytes);
        SHA256.HashData(utf8Bytes[..written], destination);
    }

    /// <summary>
    /// Serializes the beacon frame into the destination span in little-endian format.
    /// Returns true if destination has at least 72 bytes; otherwise false.
    /// </summary>
    public bool TryWrite(Span<byte> destination)
    {
        if (destination.Length < FrameSize)
            return false;

        BinaryPrimitives.WriteUInt32LittleEndian(destination[0..4], Magic);
        BinaryPrimitives.WriteUInt16LittleEndian(destination[4..6], ProtocolVersion);
        ClusterId.TryWriteBytes(destination[6..22]);
        PeerIdHash.AsSpan().CopyTo(destination[22..54]);
        BinaryPrimitives.WriteUInt16LittleEndian(destination[54..56], ListenPort);
        BinaryPrimitives.WriteUInt64LittleEndian(destination[56..64], Sequence);
        BinaryPrimitives.WriteInt64LittleEndian(destination[64..72], TimestampUnixMs);
        return true;
    }

    /// <summary>
    /// Serializes the beacon frame into a newly allocated 72-byte array.
    /// </summary>
    public byte[] ToByteArray()
    {
        var bytes = new byte[FrameSize];
        TryWrite(bytes);
        return bytes;
    }

    /// <summary>
    /// Attempts to parse a 72-byte binary beacon frame.
    /// Rejects truncated buffers or mismatched magic/version with zero heap allocations.
    /// </summary>
    public static bool TryParse(ReadOnlySpan<byte> source, [NotNullWhen(true)] out BeaconFrame? frame)
    {
        frame = null;

        if (source.Length < FrameSize)
            return false;

        uint magic = BinaryPrimitives.ReadUInt32LittleEndian(source[0..4]);
        if (magic != ExpectedMagic)
            return false;

        ushort version = BinaryPrimitives.ReadUInt16LittleEndian(source[4..6]);
        if (version != CurrentVersion)
            return false;

        Guid clusterId = new Guid(source[6..22]);
        byte[] peerIdHash = source[22..54].ToArray();
        ushort listenPort = BinaryPrimitives.ReadUInt16LittleEndian(source[54..56]);
        ulong sequence = BinaryPrimitives.ReadUInt64LittleEndian(source[56..64]);
        long timestampUnixMs = BinaryPrimitives.ReadInt64LittleEndian(source[64..72]);

        frame = new BeaconFrame(magic, version, clusterId, peerIdHash, listenPort, sequence, timestampUnixMs);
        return true;
    }

    public bool Equals(BeaconFrame? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return Magic == other.Magic &&
               ProtocolVersion == other.ProtocolVersion &&
               ClusterId == other.ClusterId &&
               ListenPort == other.ListenPort &&
               Sequence == other.Sequence &&
               TimestampUnixMs == other.TimestampUnixMs &&
               PeerIdHash.AsSpan().SequenceEqual(other.PeerIdHash.AsSpan());
    }

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Magic);
        hash.Add(ProtocolVersion);
        hash.Add(ClusterId);
        hash.Add(ListenPort);
        hash.Add(Sequence);
        hash.Add(TimestampUnixMs);
        if (PeerIdHash.Length >= 4)
        {
            hash.Add(BitConverter.ToInt32(PeerIdHash, 0));
        }
        return hash.ToHashCode();
    }

    public override string ToString() =>
        $"BeaconFrame: Cluster={ClusterId}, PeerHash={PeerIdHashHex[..8]}..., Port={ListenPort}, Seq={Sequence}, Time={TimestampUnixMs}";
}
