using DeltaSync.Core.Causality;
using DeltaSync.Core.Chunking;
using DeltaSync.Core.Models;
using DeltaSync.Core.Storage;

namespace DeltaSync.Core.Sync.Wire;

/// <summary>
/// Root Merkle probe sent across IPeerTransportChannel to test O(1) replica equality.
/// Spec §2.1.
/// </summary>
public sealed record MerkleRootProbe(Guid ClusterId, string RootHash);

/// <summary>
/// Response to MerkleRootProbe indicating whether local and remote root hashes match.
/// Spec §2.1.
/// </summary>
public sealed record MerkleRootResponse(Guid ClusterId, bool Matches, string RootHash);

/// <summary>
/// Query for directory children under a given normalized prefix ("" for root).
/// Spec §2.1. Optionally includes the caller's node hash for early O(1) equality cutoff.
/// </summary>
public sealed record PrefixDiffRequest(string Prefix, string? NodeHash = null);

/// <summary>
/// Lightweight wire representation of a tracked file metadata record.
/// </summary>
public sealed record WireFileRecord(
    string RelativePath,
    long SizeBytes,
    string RootHash,
    DateTimeOffset ModifiedUtc,
    VectorClock Clock,
    bool IsDeleted,
    int Version)
{
    public static WireFileRecord FromMetadata(FileMetadata meta) =>
        new(meta.RelativePath, meta.SizeBytes, meta.RootHash, meta.ModifiedUtc, meta.Clock, meta.IsDeleted, meta.Version);

    public FileMetadata ToMetadata() =>
        new(RelativePath, SizeBytes, RootHash, ModifiedUtc, Clock, IsDeleted, Version, ModifiedUtc);
}

/// <summary>
/// Lightweight wire representation of a Merkle prefix subtree node.
/// </summary>
public sealed record WireMerkleNodeRecord(
    string Prefix,
    string NodeHash,
    int ChildCount)
{
    public static WireMerkleNodeRecord FromNode(MerkleNode node) =>
        new(node.Prefix, node.NodeHash, node.ChildCount);

    public MerkleNode ToNode(DateTimeOffset updatedUtc = default) =>
        new(Prefix, NodeHash, ChildCount, updatedUtc == default ? DateTimeOffset.UtcNow : updatedUtc);
}

/// <summary>
/// Response to PrefixDiffRequest detailing immediate children and subdirectory hashes.
/// Spec §2.1.
/// </summary>
public sealed record PrefixDiffResponse(
    string Prefix,
    bool AreIdentical,
    IReadOnlyList<WireFileRecord> Files,
    IReadOnlyList<WireMerkleNodeRecord> Subdirectories);

/// <summary>
/// Query for the full FastCDC chunk manifest and vector clock of a specific file.
/// Spec §2.1.
/// </summary>
public sealed record FileManifestQuery(string RelativePath);

/// <summary>
/// Lightweight wire representation of a FastCDC chunk descriptor.
/// </summary>
public sealed record WireChunkRecord(
    int Index,
    long Offset,
    int Length,
    string HashHex)
{
    public static WireChunkRecord FromDescriptor(ChunkDescriptor desc) =>
        new(desc.Index, desc.Offset, desc.Length, desc.HashHex);

    public ChunkDescriptor ToDescriptor() =>
        new(Index, Offset, Length, Convert.FromHexString(HashHex));
}

/// <summary>
/// Response containing file metadata, content hash, total bytes, vector clock, and chunk fingerprints.
/// Spec §2.1.
/// </summary>
public sealed record FileManifestResponse(
    string RelativePath,
    string ContentHash,
    long TotalBytes,
    VectorClock Clock,
    IReadOnlyList<WireChunkRecord> Chunks)
{
    public FileManifest ToFileManifest()
    {
        var descriptors = Chunks.Select(c => c.ToDescriptor()).ToList();
        return new FileManifest(RelativePath, TotalBytes, ContentHash, descriptors);
    }

    public static FileManifestResponse FromFileManifest(FileManifest manifest, VectorClock clock)
    {
        var chunks = manifest.Chunks.Select(WireChunkRecord.FromDescriptor).ToList();
        return new FileManifestResponse(manifest.RelativePath, manifest.RootHash, manifest.FileSize, clock, chunks);
    }
}

/// <summary>
/// Pipelined request for a single missing chunk payload by its SHA-256 hash hex.
/// Spec §2.1.
/// </summary>
public sealed record ChunkFetchRequest(string ChunkHash);

/// <summary>
/// Response containing chunk SHA-256 hash hex and raw binary chunk payload.
/// Spec §2.1.
/// </summary>
public sealed record ChunkPayloadResponse(string ChunkHash, ReadOnlyMemory<byte> Payload);

/// <summary>
/// Notification that synchronization has concluded or reached steady state.
/// Spec §2.1.
/// </summary>
public sealed record SyncCompletedNotice(Guid ClusterId, string PeerId);
