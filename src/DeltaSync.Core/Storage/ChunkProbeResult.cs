namespace DeltaSync.Core.Storage;

/// <summary>
/// Partitioned result of probing chunk presence in the local state store.
/// </summary>
public sealed record ChunkProbeResult(
    IReadOnlySet<string> LocalHashes,
    IReadOnlySet<string> MissingHashes
)
{
    public int LocalCount => LocalHashes.Count;
    public int MissingCount => MissingHashes.Count;
    public int TotalCount => LocalCount + MissingCount;
    public bool IsComplete => MissingCount == 0;
}
