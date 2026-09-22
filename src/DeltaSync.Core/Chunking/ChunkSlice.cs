namespace DeltaSync.Core.Chunking;

/// <summary>
/// Represents the byte offset and length of an identified chunk within a stream or buffer.
/// </summary>
public readonly record struct ChunkSlice(long Offset, int Length);
