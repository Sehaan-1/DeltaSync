namespace DeltaSync.Core.Chunking;

/// <summary>
/// Exception thrown when cryptographic verification of a chunk payload or whole-file root hash fails.
/// </summary>
public class ChunkIntegrityException : Exception
{
    public int? ChunkIndex { get; init; }
    public string? ExpectedHash { get; init; }
    public string? ActualHash { get; init; }

    public ChunkIntegrityException(string message)
        : base(message)
    {
    }

    public ChunkIntegrityException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    public ChunkIntegrityException(int chunkIndex, string expectedHash, string actualHash)
        : base($"Chunk integrity verification failed for chunk index {chunkIndex}. Expected SHA-256: {expectedHash}, Actual: {actualHash}")
    {
        ChunkIndex = chunkIndex;
        ExpectedHash = expectedHash.ToLowerInvariant();
        ActualHash = actualHash.ToLowerInvariant();
    }

    public static ChunkIntegrityException RootHashMismatch(string expectedRootHash, string actualRootHash)
    {
        return new ChunkIntegrityException(
            $"Whole-file root hash verification failed. Expected SHA-256: {expectedRootHash.ToLowerInvariant()}, Actual: {actualRootHash.ToLowerInvariant()}")
        {
            ExpectedHash = expectedRootHash.ToLowerInvariant(),
            ActualHash = actualRootHash.ToLowerInvariant()
        };
    }
}
