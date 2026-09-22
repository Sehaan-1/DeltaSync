using DeltaSync.Core.Causality;

namespace DeltaSync.Core.Models;

/// <summary>
/// Immutable metadata record describing a tracked file, its causal vector clock,
/// content identity, and synchronization state.
/// </summary>
public sealed record FileMetadata
{
    public string RelativePath { get; init; }
    public long SizeBytes { get; init; }
    public string RootHash { get; init; }
    public DateTimeOffset ModifiedUtc { get; init; }
    public VectorClock Clock { get; init; }
    public bool IsDeleted { get; init; }
    public int Version { get; init; }
    public DateTimeOffset UpdatedUtc { get; init; }

    public FileMetadata(
        string relativePath,
        long sizeBytes,
        string rootHash,
        DateTimeOffset modifiedUtc,
        VectorClock? clock = null,
        bool isDeleted = false,
        int version = 1,
        DateTimeOffset? updatedUtc = null)
    {
        RelativePath = relativePath ?? throw new ArgumentNullException(nameof(relativePath));
        SizeBytes = sizeBytes;
        RootHash = rootHash ?? throw new ArgumentNullException(nameof(rootHash));
        ModifiedUtc = modifiedUtc;
        Clock = clock ?? VectorClock.Empty;
        IsDeleted = isDeleted;
        Version = version;
        UpdatedUtc = updatedUtc ?? modifiedUtc;
    }
}
