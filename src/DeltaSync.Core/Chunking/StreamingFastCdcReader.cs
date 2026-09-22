using System.Runtime.CompilerServices;
using System.Security.Cryptography;

namespace DeltaSync.Core.Chunking;

/// <summary>
/// A chunk produced during streaming FastCDC processing, containing chunk descriptor and byte payload.
/// </summary>
public readonly record struct ChunkData(ChunkDescriptor Descriptor, ReadOnlyMemory<byte> Payload)
{
    public int Index => Descriptor.Index;
    public long Offset => Descriptor.Offset;
    public int Length => Descriptor.Length;
    public string HashHex => Descriptor.HashHex;
}

/// <summary>
/// Stream-based FastCDC boundary reader that partitions arbitrary input streams into content-defined chunks
/// using a bounded sliding window buffer (default 512 KB = 2 * MaxChunkSize).
/// Guarantees bounded heap memory (< 2 MB) regardless of input stream size.
/// </summary>
public static class StreamingFastCdcReader
{
    public const int DefaultBufferSize = 512 * 1024; // 512 KB (2 * 256 KB MaxSize)

    /// <summary>
    /// Synchronously reads chunks from an arbitrary Stream using a bounded sliding window buffer.
    /// </summary>
    public static IEnumerable<ChunkData> ReadChunks(Stream stream, FastCdcConfig? config = null, int bufferSize = DefaultBufferSize)
    {
        ArgumentNullException.ThrowIfNull(stream);

        var cfg = config ?? FastCdcConfig.Default;
        int capacity = Math.Max(bufferSize, cfg.MaxSize * 2);
        byte[] buffer = new byte[capacity];

        int offset = 0;
        int count = 0;
        long globalOffset = 0;
        int chunkIndex = 0;
        bool isEof = false;

        while (true)
        {
            if (count < cfg.MaxSize && !isEof)
            {
                if (offset > 0)
                {
                    if (count > 0)
                    {
                        buffer.AsSpan(offset, count).CopyTo(buffer.AsSpan(0, count));
                    }
                    offset = 0;
                }

                while (count < capacity)
                {
                    int bytesRead = stream.Read(buffer, count, capacity - count);
                    if (bytesRead == 0)
                    {
                        isEof = true;
                        break;
                    }
                    count += bytesRead;
                }
            }

            if (count == 0)
            {
                break;
            }

            var (chunkLen, descriptor, payload) = CutChunk(buffer, offset, count, chunkIndex, globalOffset, cfg);
            yield return new ChunkData(descriptor, payload);

            chunkIndex++;
            globalOffset += chunkLen;
            offset += chunkLen;
            count -= chunkLen;
        }
    }

    /// <summary>
    /// Asynchronously reads chunks from an arbitrary Stream using a bounded sliding window buffer.
    /// </summary>
    public static async IAsyncEnumerable<ChunkData> ReadChunksAsync(
        Stream stream,
        FastCdcConfig? config = null,
        int bufferSize = DefaultBufferSize,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);

        var cfg = config ?? FastCdcConfig.Default;
        int capacity = Math.Max(bufferSize, cfg.MaxSize * 2);
        byte[] buffer = new byte[capacity];

        int offset = 0;
        int count = 0;
        long globalOffset = 0;
        int chunkIndex = 0;
        bool isEof = false;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (count < cfg.MaxSize && !isEof)
            {
                if (offset > 0)
                {
                    if (count > 0)
                    {
                        buffer.AsSpan(offset, count).CopyTo(buffer.AsSpan(0, count));
                    }
                    offset = 0;
                }

                while (count < capacity)
                {
                    int bytesRead = await stream.ReadAsync(buffer.AsMemory(count, capacity - count), cancellationToken);
                    if (bytesRead == 0)
                    {
                        isEof = true;
                        break;
                    }
                    count += bytesRead;
                }
            }

            if (count == 0)
            {
                break;
            }

            var (chunkLen, descriptor, payload) = CutChunk(buffer, offset, count, chunkIndex, globalOffset, cfg);
            yield return new ChunkData(descriptor, payload);

            chunkIndex++;
            globalOffset += chunkLen;
            offset += chunkLen;
            count -= chunkLen;
        }
    }

    /// <summary>
    /// Reads a Stream using a bounded sliding buffer, partitions it into chunks, and computes the whole-file
    /// SHA-256 root hash without allocating chunk payloads in memory.
    /// </summary>
    public static FileManifest ReadManifest(string relativePath, Stream stream, FastCdcConfig? config = null, int bufferSize = DefaultBufferSize)
    {
        if (string.IsNullOrWhiteSpace(relativePath))
            throw new ArgumentException("Relative path cannot be null, empty, or whitespace.", nameof(relativePath));
        ArgumentNullException.ThrowIfNull(stream);

        var cfg = config ?? FastCdcConfig.Default;
        int capacity = Math.Max(bufferSize, cfg.MaxSize * 2);
        byte[] buffer = new byte[capacity];

        int offset = 0;
        int count = 0;
        long globalOffset = 0;
        int chunkIndex = 0;
        bool isEof = false;

        using var incrementalHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var descriptors = new List<ChunkDescriptor>();

        while (true)
        {
            if (count < cfg.MaxSize && !isEof)
            {
                if (offset > 0)
                {
                    if (count > 0)
                    {
                        buffer.AsSpan(offset, count).CopyTo(buffer.AsSpan(0, count));
                    }
                    offset = 0;
                }

                while (count < capacity)
                {
                    int bytesRead = stream.Read(buffer, count, capacity - count);
                    if (bytesRead == 0)
                    {
                        isEof = true;
                        break;
                    }
                    count += bytesRead;
                }
            }

            if (count == 0)
            {
                break;
            }

            int chunkLen = CutAndDigestChunk(buffer, offset, count, chunkIndex, globalOffset, cfg, descriptors, incrementalHash);

            chunkIndex++;
            globalOffset += chunkLen;
            offset += chunkLen;
            count -= chunkLen;
        }

        if (descriptors.Count == 0)
        {
            return FileManifest.CreateEmpty(relativePath);
        }

        byte[] rootHashBytes = incrementalHash.GetHashAndReset();
        string rootHash = Convert.ToHexString(rootHashBytes).ToLowerInvariant();

        return new FileManifest(relativePath, globalOffset, rootHash, descriptors);
    }

    /// <summary>
    /// Asynchronously reads a Stream using a bounded sliding buffer, partitions it into chunks, and computes
    /// the whole-file SHA-256 root hash without allocating chunk payloads in memory.
    /// </summary>
    public static async Task<FileManifest> ReadManifestAsync(
        string relativePath,
        Stream stream,
        FastCdcConfig? config = null,
        int bufferSize = DefaultBufferSize,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(relativePath))
            throw new ArgumentException("Relative path cannot be null, empty, or whitespace.", nameof(relativePath));
        ArgumentNullException.ThrowIfNull(stream);

        var cfg = config ?? FastCdcConfig.Default;
        int capacity = Math.Max(bufferSize, cfg.MaxSize * 2);
        byte[] buffer = new byte[capacity];

        int offset = 0;
        int count = 0;
        long globalOffset = 0;
        int chunkIndex = 0;
        bool isEof = false;

        using var incrementalHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var descriptors = new List<ChunkDescriptor>();

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (count < cfg.MaxSize && !isEof)
            {
                if (offset > 0)
                {
                    if (count > 0)
                    {
                        buffer.AsSpan(offset, count).CopyTo(buffer.AsSpan(0, count));
                    }
                    offset = 0;
                }

                while (count < capacity)
                {
                    int bytesRead = await stream.ReadAsync(buffer.AsMemory(count, capacity - count), cancellationToken);
                    if (bytesRead == 0)
                    {
                        isEof = true;
                        break;
                    }
                    count += bytesRead;
                }
            }

            if (count == 0)
            {
                break;
            }

            int chunkLen = CutAndDigestChunk(buffer, offset, count, chunkIndex, globalOffset, cfg, descriptors, incrementalHash);

            chunkIndex++;
            globalOffset += chunkLen;
            offset += chunkLen;
            count -= chunkLen;
        }

        if (descriptors.Count == 0)
        {
            return FileManifest.CreateEmpty(relativePath);
        }

        byte[] rootHashBytes = incrementalHash.GetHashAndReset();
        string rootHash = Convert.ToHexString(rootHashBytes).ToLowerInvariant();

        return new FileManifest(relativePath, globalOffset, rootHash, descriptors);
    }

    private static (int chunkLen, ChunkDescriptor descriptor, byte[] payload) CutChunk(
        byte[] buffer, int offset, int count, int chunkIndex, long globalOffset, FastCdcConfig cfg)
    {
        int chunkLen = FastCdcChunker.NextChunk(buffer.AsSpan(offset, count), cfg);
        ReadOnlySpan<byte> chunkSpan = buffer.AsSpan(offset, chunkLen);
        byte[] hash = SHA256.HashData(chunkSpan);
        var descriptor = new ChunkDescriptor(chunkIndex, globalOffset, chunkLen, hash);
        byte[] payload = chunkSpan.ToArray();
        return (chunkLen, descriptor, payload);
    }

    private static int CutAndDigestChunk(
        byte[] buffer,
        int offset,
        int count,
        int chunkIndex,
        long globalOffset,
        FastCdcConfig cfg,
        List<ChunkDescriptor> descriptors,
        IncrementalHash incrementalHash)
    {
        int chunkLen = FastCdcChunker.NextChunk(buffer.AsSpan(offset, count), cfg);
        ReadOnlySpan<byte> chunkSpan = buffer.AsSpan(offset, chunkLen);
        byte[] hash = SHA256.HashData(chunkSpan);
        descriptors.Add(new ChunkDescriptor(chunkIndex, globalOffset, chunkLen, hash));
        incrementalHash.AppendData(chunkSpan);
        return chunkLen;
    }
}
