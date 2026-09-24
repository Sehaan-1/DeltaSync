using System.Security.Cryptography;

namespace DeltaSync.Core.Chunking;

/// <summary>
/// Telemetry and audit summary of a completed delta reconstruction operation.
/// </summary>
public sealed record ReconstructionResult(
    string DestinationPath,
    long TotalBytes,
    string RootHash,
    int TotalChunks,
    int ReusedChunks,
    int TransmittedChunks,
    long ReusedBytes,
    long TransmittedBytes)
{
    public double ReuseRatio => TotalBytes == 0 ? 1.0 : (double)ReusedBytes / TotalBytes;
    public double BandwidthSavingsRatio => TotalBytes == 0 ? 1.0 : 1.0 - ((double)TransmittedBytes / TotalBytes);
}

/// <summary>
/// High-integrity delta file reconstructor.
/// Assembles target files from local and transmitted chunks with atomic temporary replacement,
/// cryptographic chunk and whole-file root verification, and strict memory bounds (< 2 MB).
/// </summary>
public static class DeltaReconstructor
{
    public const string DefaultTempDirectoryName = ".deltasync/tmp";

    /// <summary>
    /// Asynchronously reconstructs a target file using local and remote chunks.
    /// Writes chunks into an isolated temporary file, verifies every chunk hash and whole-file root hash,
    /// and performs an atomic swap to the final destination upon successful verification.
    /// </summary>
    public static async Task<ReconstructionResult> ReconstructAsync(
        FileManifest targetManifest,
        string destinationFilePath,
        ILocalChunkProvider localChunkProvider,
        IRemoteChunkSource remoteChunkSource,
        string? tempDirectory = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(targetManifest);
        ArgumentException.ThrowIfNullOrWhiteSpace(destinationFilePath);
        ArgumentNullException.ThrowIfNull(localChunkProvider);
        ArgumentNullException.ThrowIfNull(remoteChunkSource);

        string fullDestPath = Path.GetFullPath(destinationFilePath);
        string? destDir = Path.GetDirectoryName(fullDestPath);

        string resolvedTempDir = tempDirectory != null
            ? Path.GetFullPath(tempDirectory)
            : Path.Combine(destDir ?? ".", ".deltasync", "tmp");

        Directory.CreateDirectory(resolvedTempDir);
        string tempFilePath = Path.Combine(resolvedTempDir, $"{Guid.NewGuid():N}.tmp");

        int reusedChunks = 0;
        int transmittedChunks = 0;
        long reusedBytes = 0;
        long transmittedBytes = 0;
        long totalBytesWritten = 0;

        try
        {
            using (var incrementalHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256))
            {
                // Write into isolated temporary file
                await using (var tempStream = new FileStream(
                    tempFilePath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    bufferSize: 65536,
                    useAsync: true))
                {
                    for (int i = 0; i < targetManifest.Chunks.Count; i++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        var chunk = targetManifest.Chunks[i];

                        ReadOnlyMemory<byte> chunkPayload;

                        // 1. Try local chunk provider
                        var localData = await localChunkProvider.GetChunkAsync(chunk.HashHex, cancellationToken);
                        if (localData.HasValue)
                        {
                            byte[] computedHash = SHA256.HashData(localData.Value.Span);
                            if (!computedHash.AsSpan().SequenceEqual(chunk.Hash))
                            {
                                throw new ChunkIntegrityException(
                                    chunk.Index,
                                    chunk.HashHex,
                                    Convert.ToHexString(computedHash).ToLowerInvariant());
                            }

                            chunkPayload = localData.Value;
                            reusedChunks++;
                            reusedBytes += chunk.Length;
                        }
                        else
                        {
                            // 2. Fetch missing chunk from remote source
                            var remoteData = await remoteChunkSource.FetchChunkAsync(chunk, cancellationToken);
                            byte[] computedHash = SHA256.HashData(remoteData.Span);
                            if (!computedHash.AsSpan().SequenceEqual(chunk.Hash))
                            {
                                throw new ChunkIntegrityException(
                                    chunk.Index,
                                    chunk.HashHex,
                                    Convert.ToHexString(computedHash).ToLowerInvariant());
                            }

                            chunkPayload = remoteData;
                            transmittedChunks++;
                            transmittedBytes += chunk.Length;
                        }

                        await tempStream.WriteAsync(chunkPayload, cancellationToken);
                        incrementalHash.AppendData(chunkPayload.Span);
                        totalBytesWritten += chunk.Length;
                    }

                    await tempStream.FlushAsync(cancellationToken);
                }

                // Verify whole-file SHA-256 root hash
                byte[] computedRootBytes = incrementalHash.GetHashAndReset();
                string computedRootHex = Convert.ToHexString(computedRootBytes).ToLowerInvariant();

                if (!string.Equals(computedRootHex, targetManifest.RootHash, StringComparison.OrdinalIgnoreCase))
                {
                    throw ChunkIntegrityException.RootHashMismatch(targetManifest.RootHash, computedRootHex);
                }
            }

            // Ensure destination folder exists
            if (destDir != null)
            {
                Directory.CreateDirectory(destDir);
            }

            // Atomic file swap
            File.Move(tempFilePath, fullDestPath, overwrite: true);

            return new ReconstructionResult(
                fullDestPath,
                totalBytesWritten,
                targetManifest.RootHash,
                targetManifest.Chunks.Count,
                reusedChunks,
                transmittedChunks,
                reusedBytes,
                transmittedBytes);
        }
        finally
        {
            // Defensive cleanup: remove temporary file if it was not atomically moved to destination
            if (File.Exists(tempFilePath))
            {
                try
                {
                    File.Delete(tempFilePath);
                }
                catch
                {
                    // Suppress cleanup exception to avoid masking root cause exception
                }
            }
        }
    }

    /// <summary>
    /// Synchronously reconstructs a target file.
    /// </summary>
    /// <remarks>
    /// M-06: This overload blocks a thread-pool thread for the full IO duration and can deadlock
    /// in synchronization contexts (ASP.NET Framework, WinForms). Prefer <see cref="ReconstructAsync"/>.
    /// </remarks>
    [Obsolete("Sync-over-async can deadlock. Use ReconstructAsync instead.", error: false)]
    public static ReconstructionResult Reconstruct(
        FileManifest targetManifest,
        string destinationFilePath,
        ILocalChunkProvider localChunkProvider,
        IRemoteChunkSource remoteChunkSource,
        string? tempDirectory = null)
    {
        // Runs on thread pool to avoid SynchronizationContext deadlock but still blocks the caller.
        return Task.Run(() => ReconstructAsync(
                targetManifest, destinationFilePath, localChunkProvider, remoteChunkSource, tempDirectory, CancellationToken.None))
            .GetAwaiter()
            .GetResult();
    }
}
