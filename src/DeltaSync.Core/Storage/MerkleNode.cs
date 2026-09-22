namespace DeltaSync.Core.Storage;

/// <summary>
/// Represents a node in the hierarchical directory Merkle prefix tree.
/// Follows Spec §3 Step 6 and Honors ADR-0003.
/// </summary>
/// <param name="Prefix">The normalized directory prefix (e.g. "" for root, "docs", "docs/arch").</param>
/// <param name="NodeHash">The 64-character lowercase hexadecimal SHA-256 digest of this directory prefix.</param>
/// <param name="ChildCount">Total active files contained in this directory and all its subdirectories.</param>
/// <param name="UpdatedUtc">The UTC timestamp when this node was calculated.</param>
public sealed record MerkleNode(
    string Prefix,
    string NodeHash,
    int ChildCount,
    DateTimeOffset UpdatedUtc
);
