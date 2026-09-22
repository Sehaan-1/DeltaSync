using System.Security.Cryptography;

namespace DeltaSync.Core.Chunking;

/// <summary>
/// High-throughput cryptographic fingerprinter and manifest differencing engine for FastCDC chunks.
/// </summary>
public static class ChunkFingerprinter
{
    /// <summary>
    /// Partitions an in-memory byte buffer using FastCDC, computes SHA-256 fingerprints for each chunk,
    /// and generates an immutable FileManifest including the whole-file root SHA-256 hash.
    /// </summary>
    public static FileManifest CreateManifest(string relativePath, ReadOnlyMemory<byte> data, FastCdcConfig? config = null)
    {
        if (string.IsNullOrWhiteSpace(relativePath))
            throw new ArgumentException("Relative path cannot be null, empty, or whitespace.", nameof(relativePath));

        if (data.IsEmpty)
        {
            return FileManifest.CreateEmpty(relativePath);
        }

        var cfg = config ?? FastCdcConfig.Default;
        var slices = FastCdcChunker.Chunk(data, cfg);
        var descriptors = FingerprintSlices(data.Span, slices);

        byte[] rootHashBytes = SHA256.HashData(data.Span);
        string rootHash = Convert.ToHexString(rootHashBytes).ToLowerInvariant();

        return new FileManifest(relativePath, data.Length, rootHash, descriptors);
    }

    /// <summary>
    /// Reads a Stream to completion, partitions into FastCDC chunks, and generates a FileManifest.
    /// </summary>
    public static FileManifest CreateManifest(string relativePath, Stream stream, FastCdcConfig? config = null)
    {
        ArgumentNullException.ThrowIfNull(stream);

        if (stream is MemoryStream ms && ms.TryGetBuffer(out var seg))
        {
            return CreateManifest(relativePath, seg.AsMemory(), config);
        }

        using var memory = new MemoryStream();
        stream.CopyTo(memory);
        return CreateManifest(relativePath, memory.ToArray(), config);
    }

    /// <summary>
    /// Computes 32-byte SHA-256 digests for each provided chunk slice against a byte span.
    /// </summary>
    public static IReadOnlyList<ChunkDescriptor> FingerprintSlices(ReadOnlySpan<byte> buffer, IReadOnlyList<ChunkSlice> slices)
    {
        ArgumentNullException.ThrowIfNull(slices);

        var descriptors = new ChunkDescriptor[slices.Count];
        for (int i = 0; i < slices.Count; i++)
        {
            var slice = slices[i];
            ReadOnlySpan<byte> chunkSpan = buffer.Slice((int)slice.Offset, slice.Length);
            byte[] hash = SHA256.HashData(chunkSpan);
            descriptors[i] = new ChunkDescriptor(i, slice.Offset, slice.Length, hash);
        }

        return descriptors;
    }

    /// <summary>
    /// Compares a remote file manifest with a local file manifest to determine the set of missing
    /// chunk indices that must be requested from the remote peer.
    /// </summary>
    public static ManifestDiff ComputeDiff(FileManifest remoteManifest, FileManifest? localManifest)
    {
        ArgumentNullException.ThrowIfNull(remoteManifest);

        if (localManifest is null)
        {
            return ComputeMissingAll(remoteManifest);
        }

        // Fast path: root hashes match bit-for-bit
        if (string.Equals(remoteManifest.RootHash, localManifest.RootHash, StringComparison.OrdinalIgnoreCase))
        {
            return new ManifestDiff(
                Array.Empty<int>(),
                Array.Empty<ChunkDescriptor>(),
                remoteManifest.Chunks,
                0,
                remoteManifest.FileSize);
        }

        var localHashes = new HashSet<string>(localManifest.Chunks.Count, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < localManifest.Chunks.Count; i++)
        {
            localHashes.Add(localManifest.Chunks[i].HashHex);
        }

        return ComputeDiff(remoteManifest, localHashes);
    }

    /// <summary>
    /// Compares a remote file manifest with a set of known available chunk hashes (e.g. from local SQLite store or chunk cache).
    /// </summary>
    public static ManifestDiff ComputeDiff(FileManifest remoteManifest, IReadOnlySet<string> availableChunkHashesHex)
    {
        ArgumentNullException.ThrowIfNull(remoteManifest);
        ArgumentNullException.ThrowIfNull(availableChunkHashesHex);

        var missingIndices = new List<int>();
        var missingChunks = new List<ChunkDescriptor>();
        var matchedChunks = new List<ChunkDescriptor>();
        long missingBytes = 0;
        long reusedBytes = 0;

        for (int i = 0; i < remoteManifest.Chunks.Count; i++)
        {
            var chunk = remoteManifest.Chunks[i];
            if (availableChunkHashesHex.Contains(chunk.HashHex))
            {
                matchedChunks.Add(chunk);
                reusedBytes += chunk.Length;
            }
            else
            {
                missingIndices.Add(chunk.Index);
                missingChunks.Add(chunk);
                missingBytes += chunk.Length;
            }
        }

        return new ManifestDiff(
            missingIndices,
            missingChunks,
            matchedChunks,
            missingBytes,
            reusedBytes);
    }

    private static ManifestDiff ComputeMissingAll(FileManifest remoteManifest)
    {
        var missingIndices = new int[remoteManifest.Chunks.Count];
        for (int i = 0; i < remoteManifest.Chunks.Count; i++)
        {
            missingIndices[i] = i;
        }

        return new ManifestDiff(
            missingIndices,
            remoteManifest.Chunks,
            Array.Empty<ChunkDescriptor>(),
            remoteManifest.FileSize,
            0);
    }
}
