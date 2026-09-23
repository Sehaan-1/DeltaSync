using DeltaSync.Core.Causality;
using DeltaSync.Core.Chunking;
using DeltaSync.Core.Sync.Wire;
using DeltaSync.Network;

namespace DeltaSync.Core.Sync;

/// <summary>
/// Summary of a file identified as divergent during prefix reconciliation.
/// </summary>
public sealed record DivergentFileSummary(
    string RelativePath,
    string? RemoteRootHash,
    string? LocalRootHash,
    long RemoteSizeBytes,
    VectorClock? RemoteClock,
    bool RemoteIsDeleted)
{
    public bool IsMissingLocally => LocalRootHash == null;
    public bool IsContentModified => LocalRootHash != null && !string.Equals(LocalRootHash, RemoteRootHash, StringComparison.OrdinalIgnoreCase);
}

/// <summary>
/// Wire protocol port orchestrating Merkle anti-entropy probe exchange, hierarchical prefix traversal,
/// manifest queries, and sliding-window chunk streaming over IPeerTransportChannel.
/// Spec §2.2, §3 Steps 2–3.
/// </summary>
public interface ISyncWireProtocol : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Attaches to an IPeerTransportChannel to handle incoming wire requests and route response frames.
    /// Returns an IAsyncDisposable registration that detaches when disposed.
    /// </summary>
    IAsyncDisposable AttachChannel(IPeerTransportChannel channel);

    /// <summary>
    /// Evaluates O(1) equality between local and remote Merkle roots over the transport channel.
    /// Returns true if roots match exactly (0 bytes/chunks needed); false if divergent.
    /// </summary>
    Task<bool> ProbeRootEqualityAsync(IPeerTransportChannel channel, CancellationToken ct = default);

    /// <summary>
    /// Recursively reconciles divergent prefix subtrees in O(log N) network roundtrips,
    /// skipping identical subtrees without descending into child files.
    /// </summary>
    Task<IReadOnlyList<DivergentFileSummary>> ReconcilePrefixAsync(IPeerTransportChannel channel, string prefix, CancellationToken ct = default);

    /// <summary>
    /// Fetches the file manifest (ordered chunk fingerprints and vector clock) for a specific relative path.
    /// </summary>
    Task<FileManifestResponse> FetchManifestAsync(IPeerTransportChannel channel, string relativePath, CancellationToken ct = default);

    /// <summary>
    /// Fetches a single chunk payload by its SHA-256 hash.
    /// </summary>
    Task<ReadOnlyMemory<byte>> FetchChunkAsync(IPeerTransportChannel channel, string chunkHash, CancellationToken ct = default);

    /// <summary>
    /// Concurrently fetches missing chunks using a sliding window (W = 16) to saturate link throughput.
    /// </summary>
    Task<IReadOnlyDictionary<string, ReadOnlyMemory<byte>>> FetchChunksPipelinedAsync(
        IPeerTransportChannel channel,
        IReadOnlyList<string> missingChunkHashes,
        int windowSize = 16,
        CancellationToken ct = default);

    /// <summary>
    /// Reconstructs a file using local chunks and pipelined remote chunks, staging into a temporary file
    /// and atomically verifying cryptographic integrity before replacement.
    /// </summary>
    Task<ReconstructionResult> FetchAndReconstructFileAsync(
        IPeerTransportChannel channel,
        FileManifestResponse remoteManifest,
        string destinationFilePath,
        string? tempDirectory = null,
        CancellationToken ct = default);
}
