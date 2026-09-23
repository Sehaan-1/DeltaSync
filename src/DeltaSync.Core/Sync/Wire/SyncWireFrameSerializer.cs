using System.Buffers.Binary;
using System.Text.Json;

namespace DeltaSync.Core.Sync.Wire;

/// <summary>
/// Envelope containing deserialized message frame, its message type, and correlation ID.
/// </summary>
public sealed record SyncWireFrame(SyncMessageType Type, ulong CorrelationId, object Message);

/// <summary>
/// High-performance, deterministic binary and compact JSON framing serializer for DeltaSync wire protocol.
/// Spec §2.1.
/// Frame layout:
///   Offset 0:      MessageType (1 byte)
///   Offset 1..8:   CorrelationId (8 bytes, little-endian ulong)
///   Offset 9..End: Payload (raw binary for chunk responses; compact UTF-8 JSON for metadata messages)
/// </summary>
public static class SyncWireFrameSerializer
{
    public const int HeaderSize = 9;
    public const int MaxFrameSize = 4 * 1024 * 1024; // 4 MB safety limit
    public const int HashBinarySize = 32;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        WriteIndented = false
    };

    /// <summary>
    /// Serializes a wire message into a framed binary buffer.
    /// </summary>
    public static ReadOnlyMemory<byte> Serialize(SyncMessageType type, ulong correlationId, object message)
    {
        ArgumentNullException.ThrowIfNull(message);

        return message switch
        {
            ChunkPayloadResponse payloadResp => SerializeChunkPayloadResponse(correlationId, payloadResp),
            ChunkFetchRequest chunkReq => SerializeChunkFetchRequest(correlationId, chunkReq),
            MerkleRootProbe probe => SerializeJson(SyncMessageType.MerkleRootProbe, correlationId, probe),
            MerkleRootResponse resp => SerializeJson(SyncMessageType.MerkleRootResponse, correlationId, resp),
            PrefixDiffRequest req => SerializeJson(SyncMessageType.PrefixDiffRequest, correlationId, req),
            PrefixDiffResponse resp => SerializeJson(SyncMessageType.PrefixDiffResponse, correlationId, resp),
            FileManifestQuery query => SerializeJson(SyncMessageType.FileManifestQuery, correlationId, query),
            FileManifestResponse resp => SerializeJson(SyncMessageType.FileManifestResponse, correlationId, resp),
            SyncCompletedNotice notice => SerializeJson(SyncMessageType.SyncCompletedNotice, correlationId, notice),
            _ => throw new ArgumentException($"Unsupported wire message type: {message.GetType().Name}", nameof(message))
        };
    }

    /// <summary>
    /// Deserializes a discrete binary frame received from IPeerTransportChannel.
    /// </summary>
    public static SyncWireFrame Deserialize(ReadOnlyMemory<byte> frame)
    {
        if (frame.Length < HeaderSize)
        {
            throw new InvalidDataException($"Frame too short ({frame.Length} bytes). Minimum is {HeaderSize} bytes.");
        }

        if (frame.Length > MaxFrameSize)
        {
            throw new InvalidDataException($"Frame exceeds maximum allowed size ({frame.Length} bytes > {MaxFrameSize} bytes).");
        }

        var span = frame.Span;
        byte typeByte = span[0];
        if (!Enum.IsDefined(typeof(SyncMessageType), typeByte))
        {
            throw new InvalidDataException($"Unknown message type discriminator: 0x{typeByte:X2}.");
        }

        var messageType = (SyncMessageType)typeByte;
        ulong correlationId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(1, 8));
        var payloadSpan = span.Slice(HeaderSize);

        object message = messageType switch
        {
            SyncMessageType.ChunkPayloadResponse => DeserializeChunkPayloadResponse(frame, correlationId),
            SyncMessageType.ChunkFetchRequest => DeserializeChunkFetchRequest(span, correlationId),
            SyncMessageType.MerkleRootProbe => JsonSerializer.Deserialize<MerkleRootProbe>(payloadSpan, JsonOptions)
                ?? throw new InvalidDataException("Failed to deserialize MerkleRootProbe."),
            SyncMessageType.MerkleRootResponse => JsonSerializer.Deserialize<MerkleRootResponse>(payloadSpan, JsonOptions)
                ?? throw new InvalidDataException("Failed to deserialize MerkleRootResponse."),
            SyncMessageType.PrefixDiffRequest => ValidatePrefixDiffRequest(
                JsonSerializer.Deserialize<PrefixDiffRequest>(payloadSpan, JsonOptions)
                ?? throw new InvalidDataException("Failed to deserialize PrefixDiffRequest.")),
            SyncMessageType.PrefixDiffResponse => JsonSerializer.Deserialize<PrefixDiffResponse>(payloadSpan, JsonOptions)
                ?? throw new InvalidDataException("Failed to deserialize PrefixDiffResponse."),
            SyncMessageType.FileManifestQuery => ValidateFileManifestQuery(
                JsonSerializer.Deserialize<FileManifestQuery>(payloadSpan, JsonOptions)
                ?? throw new InvalidDataException("Failed to deserialize FileManifestQuery.")),
            SyncMessageType.FileManifestResponse => JsonSerializer.Deserialize<FileManifestResponse>(payloadSpan, JsonOptions)
                ?? throw new InvalidDataException("Failed to deserialize FileManifestResponse."),
            SyncMessageType.SyncCompletedNotice => JsonSerializer.Deserialize<SyncCompletedNotice>(payloadSpan, JsonOptions)
                ?? throw new InvalidDataException("Failed to deserialize SyncCompletedNotice."),
            _ => throw new InvalidDataException($"Unhandled message type: {messageType}.")
        };

        return new SyncWireFrame(messageType, correlationId, message);
    }

    private static ReadOnlyMemory<byte> SerializeChunkPayloadResponse(ulong correlationId, ChunkPayloadResponse response)
    {
        ArgumentNullException.ThrowIfNull(response);
        byte[] hashBytes = Convert.FromHexString(response.ChunkHash);
        if (hashBytes.Length != HashBinarySize)
        {
            throw new ArgumentException($"Expected {HashBinarySize}-byte SHA-256 hash, got {hashBytes.Length} bytes.", nameof(response));
        }

        byte[] buffer = new byte[HeaderSize + HashBinarySize + response.Payload.Length];
        buffer[0] = (byte)SyncMessageType.ChunkPayloadResponse;
        BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(1, 8), correlationId);
        hashBytes.CopyTo(buffer.AsSpan(HeaderSize, HashBinarySize));
        response.Payload.Span.CopyTo(buffer.AsSpan(HeaderSize + HashBinarySize));

        return buffer;
    }

    private static ReadOnlyMemory<byte> SerializeChunkFetchRequest(ulong correlationId, ChunkFetchRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        byte[] hashBytes = Convert.FromHexString(request.ChunkHash);
        if (hashBytes.Length != HashBinarySize)
        {
            throw new ArgumentException($"Expected {HashBinarySize}-byte SHA-256 hash, got {hashBytes.Length} bytes.", nameof(request));
        }

        byte[] buffer = new byte[HeaderSize + HashBinarySize];
        buffer[0] = (byte)SyncMessageType.ChunkFetchRequest;
        BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(1, 8), correlationId);
        hashBytes.CopyTo(buffer.AsSpan(HeaderSize, HashBinarySize));

        return buffer;
    }

    private static ReadOnlyMemory<byte> SerializeJson<T>(SyncMessageType type, ulong correlationId, T message)
    {
        byte[] jsonBytes = JsonSerializer.SerializeToUtf8Bytes(message, JsonOptions);
        byte[] buffer = new byte[HeaderSize + jsonBytes.Length];
        buffer[0] = (byte)type;
        BinaryPrimitives.WriteUInt64LittleEndian(buffer.AsSpan(1, 8), correlationId);
        jsonBytes.CopyTo(buffer.AsSpan(HeaderSize));

        return buffer;
    }

    private static ChunkPayloadResponse DeserializeChunkPayloadResponse(ReadOnlyMemory<byte> frame, ulong correlationId)
    {
        if (frame.Length < HeaderSize + HashBinarySize)
        {
            throw new InvalidDataException($"ChunkPayloadResponse frame too short ({frame.Length} bytes). Expected at least {HeaderSize + HashBinarySize} bytes.");
        }

        var hashSpan = frame.Span.Slice(HeaderSize, HashBinarySize);
        string chunkHash = Convert.ToHexString(hashSpan).ToLowerInvariant();
        var payload = frame.Slice(HeaderSize + HashBinarySize);

        return new ChunkPayloadResponse(chunkHash, payload);
    }

    private static ChunkFetchRequest DeserializeChunkFetchRequest(ReadOnlySpan<byte> span, ulong correlationId)
    {
        if (span.Length < HeaderSize + HashBinarySize)
        {
            throw new InvalidDataException($"ChunkFetchRequest frame too short ({span.Length} bytes). Expected {HeaderSize + HashBinarySize} bytes.");
        }

        var hashSpan = span.Slice(HeaderSize, HashBinarySize);
        string chunkHash = Convert.ToHexString(hashSpan).ToLowerInvariant();

        return new ChunkFetchRequest(chunkHash);
    }

    private static PrefixDiffRequest ValidatePrefixDiffRequest(PrefixDiffRequest request)
    {
        ValidatePathSafety(request.Prefix, allowEmpty: true);
        return request;
    }

    private static FileManifestQuery ValidateFileManifestQuery(FileManifestQuery query)
    {
        ValidatePathSafety(query.RelativePath, allowEmpty: false);
        return query;
    }

    private static void ValidatePathSafety(string? path, bool allowEmpty)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            if (allowEmpty) return;
            throw new ArgumentException("Path cannot be empty or whitespace.", nameof(path));
        }

        if (path.Contains("..") || path.Contains(':') || path.StartsWith('/') || path.StartsWith('\\'))
        {
            throw new ArgumentException($"Invalid relative path containing path traversal or drive characters: '{path}'");
        }
    }
}
