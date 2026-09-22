namespace DeltaSync.Core.Storage;

/// <summary>
/// Physical location of a chunk payload within a tracked file,
/// enabling zero-redundancy, direct slice reads via ILocalChunkProvider.
/// </summary>
public sealed record ChunkLocation(
    string RelativePath,
    long Offset,
    int Length
);
