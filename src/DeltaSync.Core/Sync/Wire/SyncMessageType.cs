namespace DeltaSync.Core.Sync.Wire;

/// <summary>
/// Identifies the discrete message frame type in the DeltaSync wire protocol (Spec §2.1).
/// Single-byte discriminator transmitted at offset 0 of every wire frame.
/// </summary>
public enum SyncMessageType : byte
{
    /// <summary>
    /// Probe sent by initiator with local Merkle root hash to test O(1) replica equality.
    /// </summary>
    MerkleRootProbe = 0x01,

    /// <summary>
    /// Response to MerkleRootProbe indicating whether root hashes matched.
    /// </summary>
    MerkleRootResponse = 0x02,

    /// <summary>
    /// Query for children (files and subdirectories) under a divergent directory prefix.
    /// </summary>
    PrefixDiffRequest = 0x03,

    /// <summary>
    /// Response containing immediate child files and subdirectory Merkle nodes for a prefix.
    /// </summary>
    PrefixDiffResponse = 0x04,

    /// <summary>
    /// Query for the full FastCDC chunk manifest and vector clock of a specific file.
    /// </summary>
    FileManifestQuery = 0x05,

    /// <summary>
    /// Response containing the ordered chunk fingerprints and vector clock of a file.
    /// </summary>
    FileManifestResponse = 0x06,

    /// <summary>
    /// Pipelined request for a single missing chunk payload by its SHA-256 hash.
    /// </summary>
    ChunkFetchRequest = 0x07,

    /// <summary>
    /// Binary stream response carrying the raw chunk payload and SHA-256 hash.
    /// </summary>
    ChunkPayloadResponse = 0x08,

    /// <summary>
    /// Notification that synchronization has concluded or reached steady-state.
    /// </summary>
    SyncCompletedNotice = 0x09
}
