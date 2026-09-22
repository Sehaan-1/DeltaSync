using DeltaSync.Core.Models;

namespace DeltaSync.Core.Storage;

/// <summary>
/// Result of comparing local directory Merkle prefix state with a remote peer's node hash.
/// Enables O(M log F) hierarchical difference reconciliation across peers.
/// </summary>
/// <param name="Prefix">The directory prefix evaluated.</param>
/// <param name="AreIdentical">True if the remote node hash matches local state; false if differences exist.</param>
/// <param name="LocalNodeHash">The local SHA-256 node hash (or null if the prefix does not exist locally).</param>
/// <param name="RemoteNodeHash">The remote peer's SHA-256 node hash.</param>
/// <param name="Files">Direct child files under this directory prefix (populated when <paramref name="AreIdentical"/> is false).</param>
/// <param name="Subdirectories">Immediate child subdirectory Merkle nodes under this directory prefix (populated when <paramref name="AreIdentical"/> is false).</param>
public sealed record DirectoryDifference(
    string Prefix,
    bool AreIdentical,
    string? LocalNodeHash,
    string? RemoteNodeHash,
    IReadOnlyList<FileMetadata> Files,
    IReadOnlyList<MerkleNode> Subdirectories
);
