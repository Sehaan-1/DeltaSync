using DeltaSync.Core.Chunking;
using DeltaSync.Core.Models;

namespace DeltaSync.Core.Storage;

/// <summary>
/// High-performance embedded SQLite state store providing atomic file metadata ingestion,
/// FastCDC chunk inverted index resolution, reference-counted chunk lifecycle management,
/// and fast delta manifest probing.
/// </summary>
public interface ISqliteStateStore : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Atomically upserts file metadata and replaces its chunk mappings, updating chunk reference counts.
    /// </summary>
    Task UpsertFileAsync(FileMetadata file, IReadOnlyList<ChunkDescriptor> chunks, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves file metadata by case-insensitive relative path.
    /// </summary>
    Task<FileMetadata?> GetFileAsync(string relativePath, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves ordered chunk descriptors for a file by case-insensitive relative path.
    /// </summary>
    Task<IReadOnlyList<ChunkDescriptor>> GetFileChunksAsync(string relativePath, CancellationToken cancellationToken = default);

    /// <summary>
    /// Partitioned probe of chunk availability in the local chunk index.
    /// </summary>
    Task<ChunkProbeResult> ProbeChunksAsync(IEnumerable<string> chunkHashes, CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically marks a file deleted (tombstone) and decrements reference counts of its chunks.
    /// </summary>
    Task<bool> DeleteFileAsync(string relativePath, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves all files tracked in the state store.
    /// </summary>
    Task<IReadOnlyList<FileMetadata>> GetAllFilesAsync(bool includeDeleted = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resolves the physical file byte range location for a given chunk hash from active files.
    /// </summary>
    Task<ChunkLocation?> GetChunkLocationAsync(string chunkHash, CancellationToken cancellationToken = default);

    /// <summary>
    /// Resolves all physical file byte range candidate locations for a given chunk hash from active files.
    /// </summary>
    Task<IReadOnlyList<ChunkLocation>> GetChunkLocationsAsync(string chunkHash, CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves the Merkle node digest and child count for a given directory prefix.
    /// Root directory is represented as an empty string ("").
    /// </summary>
    Task<MerkleNode?> GetMerkleNodeAsync(string prefix, CancellationToken cancellationToken = default);

    /// <summary>
    /// Compares the local Merkle prefix state with a remote peer's node hash, returning the direct child files
    /// and immediate subdirectory nodes if divergent, or indicating identical state.
    /// </summary>
    Task<DirectoryDifference> GetDirectoryDifferenceAsync(string prefix, string remoteNodeHash, CancellationToken cancellationToken = default);
}

