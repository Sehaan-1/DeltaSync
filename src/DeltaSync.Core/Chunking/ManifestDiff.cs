namespace DeltaSync.Core.Chunking;

/// <summary>
/// Represents the delta evaluation between a remote file manifest and locally available chunks,
/// detailing which chunks must be transmitted across the network and which chunks are reused.
/// </summary>
public sealed record ManifestDiff
{
    /// <summary>
    /// Monotonically ordered indices of chunks in the remote manifest that are missing locally.
    /// </summary>
    public IReadOnlyList<int> MissingChunkIndices { get; }

    /// <summary>
    /// Descriptors of the remote chunks that are missing locally and must be transferred.
    /// </summary>
    public IReadOnlyList<ChunkDescriptor> MissingChunks { get; }

    /// <summary>
    /// Descriptors of remote chunks that were successfully matched and can be reused locally without network transfer.
    /// </summary>
    public IReadOnlyList<ChunkDescriptor> MatchedChunks { get; }

    /// <summary>
    /// Total bytes required to be transmitted over the network for missing chunks.
    /// </summary>
    public long MissingBytes { get; }

    /// <summary>
    /// Total bytes preserved from existing local data without network transmission.
    /// </summary>
    public long ReusedBytes { get; }

    public int MissingChunkCount => MissingChunkIndices.Count;
    public int MatchedChunkCount => MatchedChunks.Count;

    /// <summary>
    /// Percentage of the remote file's content (0.0 to 1.0) satisfied by existing local chunks.
    /// </summary>
    public double ReuseRatio => (ReusedBytes + MissingBytes) > 0
        ? (double)ReusedBytes / (ReusedBytes + MissingBytes)
        : 1.0;

    /// <summary>
    /// Returns true if all chunks match and zero bytes need to be transferred.
    /// </summary>
    public bool IsIdentical => MissingChunkCount == 0 && MissingBytes == 0;

    public ManifestDiff(
        IReadOnlyList<int> missingChunkIndices,
        IReadOnlyList<ChunkDescriptor> missingChunks,
        IReadOnlyList<ChunkDescriptor> matchedChunks,
        long missingBytes,
        long reusedBytes)
    {
        MissingChunkIndices = missingChunkIndices ?? throw new ArgumentNullException(nameof(missingChunkIndices));
        MissingChunks = missingChunks ?? throw new ArgumentNullException(nameof(missingChunks));
        MatchedChunks = matchedChunks ?? throw new ArgumentNullException(nameof(matchedChunks));
        MissingBytes = missingBytes;
        ReusedBytes = reusedBytes;
    }

    public override string ToString() =>
        $"ManifestDiff: Missing={MissingChunkCount} chunks ({MissingBytes} B), Reused={MatchedChunkCount} chunks ({ReusedBytes} B), ReuseRatio={ReuseRatio * 100:F1}%";
}
