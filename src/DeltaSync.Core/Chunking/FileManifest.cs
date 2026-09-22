namespace DeltaSync.Core.Chunking;

/// <summary>
/// Immutable manifest representing the content-defined chunk layout and cryptographic identity of a file.
/// </summary>
public sealed record FileManifest : IEquatable<FileManifest>
{
    /// <summary>
    /// Canonical SHA-256 hash for an empty byte stream (0 bytes).
    /// </summary>
    public const string EmptyRootHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    public string RelativePath { get; }
    public long FileSize { get; }
    public string RootHash { get; }
    public IReadOnlyList<ChunkDescriptor> Chunks { get; }

    public FileManifest(string relativePath, long fileSize, string rootHash, IReadOnlyList<ChunkDescriptor> chunks)
    {
        if (string.IsNullOrWhiteSpace(relativePath))
            throw new ArgumentException("Relative path cannot be null, empty, or whitespace.", nameof(relativePath));
        if (fileSize < 0)
            throw new ArgumentOutOfRangeException(nameof(fileSize), "File size must be non-negative.");
        if (string.IsNullOrWhiteSpace(rootHash))
            throw new ArgumentException("Root hash cannot be null, empty, or whitespace.", nameof(rootHash));
        ArgumentNullException.ThrowIfNull(chunks);

        RelativePath = NormalizePath(relativePath);
        FileSize = fileSize;
        RootHash = rootHash.Trim().ToLowerInvariant();

        if (RootHash.Length != 64)
            throw new ArgumentException("Root hash must be a 64-character lowercase hex SHA-256 string.", nameof(rootHash));

        long cumulativeOffset = 0;
        for (int i = 0; i < chunks.Count; i++)
        {
            var chunk = chunks[i] ?? throw new ArgumentException($"Chunk descriptor at index {i} cannot be null.", nameof(chunks));
            if (chunk.Index != i)
            {
                throw new ArgumentException(
                    $"Chunk descriptor at index {i} has non-monotonic index {chunk.Index}.",
                    nameof(chunks));
            }
            if (chunk.Offset != cumulativeOffset)
            {
                throw new ArgumentException(
                    $"Chunk {i} offset {chunk.Offset} does not match expected contiguous offset {cumulativeOffset}.",
                    nameof(chunks));
            }
            cumulativeOffset += chunk.Length;
        }

        if (cumulativeOffset != fileSize)
        {
            throw new ArgumentException(
                $"Sum of chunk lengths ({cumulativeOffset}) does not equal specified file size ({fileSize}).",
                nameof(chunks));
        }

        Chunks = chunks.ToArray();
    }

    /// <summary>
    /// Creates an empty manifest for a 0-byte file.
    /// </summary>
    public static FileManifest CreateEmpty(string relativePath)
    {
        return new FileManifest(relativePath, 0, EmptyRootHash, Array.Empty<ChunkDescriptor>());
    }

    public static string NormalizePath(string path)
    {
        return path.Trim().Replace('\\', '/').TrimStart('/');
    }

    public bool Equals(FileManifest? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        if (string.Compare(RelativePath, other.RelativePath, StringComparison.OrdinalIgnoreCase) != 0 ||
            FileSize != other.FileSize ||
            string.Compare(RootHash, other.RootHash, StringComparison.OrdinalIgnoreCase) != 0 ||
            Chunks.Count != other.Chunks.Count)
        {
            return false;
        }

        for (int i = 0; i < Chunks.Count; i++)
        {
            if (!Chunks[i].Equals(other.Chunks[i]))
            {
                return false;
            }
        }

        return true;
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(
            StringComparer.OrdinalIgnoreCase.GetHashCode(RelativePath),
            FileSize,
            StringComparer.OrdinalIgnoreCase.GetHashCode(RootHash),
            Chunks.Count);
    }

    public override string ToString() => $"Manifest[{RelativePath}]: Size={FileSize}, RootHash={RootHash[..8]}..., Chunks={Chunks.Count}";
}
