using System.Collections.Concurrent;
using DeltaSync.Core.Causality;
using DeltaSync.Core.Chunking;
using DeltaSync.Core.Models;

namespace DeltaSync.Core.Storage;

/// <summary>
/// Thread-safe in-memory state store providing an exact differential testing oracle for <see cref="SqliteStateStore"/>.
/// Maintains file metadata, FastCDC chunk inverted mappings, reference counts, and hierarchical Merkle prefix trees.
/// Follows Spec §8 Alternative 1 and Honors ADR-0003.
/// </summary>
public sealed class MemoryStateStore : ISqliteStateStore
{
    private readonly ConcurrentDictionary<string, StoredFile> _files = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, int> _chunkRefCounts = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, MerkleNodeRecord> _merkleNodes = new(StringComparer.OrdinalIgnoreCase);
    private readonly ReaderWriterLockSlim _lock = new(LockRecursionPolicy.NoRecursion);
    private bool _disposed;

    private sealed record StoredFile(FileMetadata Metadata, IReadOnlyList<ChunkDescriptor> Chunks);
    private sealed record MerkleNodeRecord(string NodeHash, int ChildCount, DateTimeOffset UpdatedUtc);

    public MemoryStateStore()
    {
        // Initialize root Merkle node
        _merkleNodes[string.Empty] = new MerkleNodeRecord(MerkleTreeHelper.EmptyNodeHash, 0, DateTimeOffset.UtcNow);
    }

    public Task UpsertFileAsync(FileMetadata file, IReadOnlyList<ChunkDescriptor> chunks, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(chunks);

        string normalizedPath = NormalizeAndValidatePath(file.RelativePath);
        ValidateFileMetadata(file, chunks);

        _lock.EnterWriteLock();
        try
        {
            int version = file.Version;
            DateTimeOffset now = DateTimeOffset.UtcNow;

            // Handle previous file state if existed
            if (_files.TryGetValue(normalizedPath, out var existing))
            {
                version = existing.Metadata.Version + 1;

                // Decrement ref counts of previous chunks
                foreach (var oldChunk in existing.Chunks)
                {
                    string oldHash = oldChunk.HashHex;
                    if (_chunkRefCounts.TryGetValue(oldHash, out int count))
                    {
                        if (count <= 1)
                        {
                            _chunkRefCounts.TryRemove(oldHash, out _);
                        }
                        else
                        {
                            _chunkRefCounts[oldHash] = count - 1;
                        }
                    }
                }
            }

            // Increment ref counts for new chunks
            foreach (var chunk in chunks)
            {
                string chunkHash = chunk.HashHex;
                _chunkRefCounts.AddOrUpdate(chunkHash, 1, (_, current) => current + 1);
            }

            var updatedMeta = new FileMetadata(
                normalizedPath,
                file.SizeBytes,
                file.RootHash,
                file.ModifiedUtc,
                file.Clock ?? VectorClock.Empty,
                file.IsDeleted,
                version,
                now
            );

            _files[normalizedPath] = new StoredFile(updatedMeta, chunks);

            // Incremental Merkle prefix tree calculation
            UpdateMerklePrefixes(normalizedPath);

            return Task.CompletedTask;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public Task<FileMetadata?> GetFileAsync(string relativePath, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        string normalizedPath = NormalizeAndValidatePath(relativePath);

        _lock.EnterReadLock();
        try
        {
            if (_files.TryGetValue(normalizedPath, out var stored))
            {
                return Task.FromResult<FileMetadata?>(stored.Metadata);
            }
            return Task.FromResult<FileMetadata?>(null);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public Task<IReadOnlyList<ChunkDescriptor>> GetFileChunksAsync(string relativePath, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        string normalizedPath = NormalizeAndValidatePath(relativePath);

        _lock.EnterReadLock();
        try
        {
            if (_files.TryGetValue(normalizedPath, out var stored) && !stored.Metadata.IsDeleted)
            {
                return Task.FromResult(stored.Chunks);
            }
            return Task.FromResult<IReadOnlyList<ChunkDescriptor>>(Array.Empty<ChunkDescriptor>());
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public Task<ChunkProbeResult> ProbeChunksAsync(IEnumerable<string> chunkHashes, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(chunkHashes);

        var uniqueHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var hash in chunkHashes)
        {
            if (string.IsNullOrWhiteSpace(hash))
                continue;

            string normalized = hash.Trim().ToLowerInvariant();
            if (normalized.Length != 64)
            {
                throw new ArgumentException($"Invalid chunk hash '{hash}'. Must be 64 hexadecimal characters.", nameof(chunkHashes));
            }
            uniqueHashes.Add(normalized);
        }

        if (uniqueHashes.Count == 0)
        {
            return Task.FromResult(new ChunkProbeResult(new HashSet<string>(), new HashSet<string>()));
        }

        var localHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var missingHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        _lock.EnterReadLock();
        try
        {
            foreach (var hash in uniqueHashes)
            {
                if (_chunkRefCounts.TryGetValue(hash, out int count) && count > 0)
                {
                    localHashes.Add(hash);
                }
                else
                {
                    missingHashes.Add(hash);
                }
            }

            return Task.FromResult(new ChunkProbeResult(localHashes, missingHashes));
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public Task<bool> DeleteFileAsync(string relativePath, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        string normalizedPath = NormalizeAndValidatePath(relativePath);

        _lock.EnterWriteLock();
        try
        {
            if (!_files.TryGetValue(normalizedPath, out var stored) || stored.Metadata.IsDeleted)
            {
                return Task.FromResult(false);
            }

            // Decrement ref counts
            foreach (var chunk in stored.Chunks)
            {
                string chunkHash = chunk.HashHex;
                if (_chunkRefCounts.TryGetValue(chunkHash, out int count))
                {
                    if (count <= 1)
                    {
                        _chunkRefCounts.TryRemove(chunkHash, out _);
                    }
                    else
                    {
                        _chunkRefCounts[chunkHash] = count - 1;
                    }
                }
            }

            // Tombstone file
            var tombstone = new FileMetadata(
                normalizedPath,
                0,
                stored.Metadata.RootHash,
                stored.Metadata.ModifiedUtc,
                stored.Metadata.Clock ?? VectorClock.Empty,
                isDeleted: true,
                version: stored.Metadata.Version + 1,
                updatedUtc: DateTimeOffset.UtcNow
            );

            _files[normalizedPath] = new StoredFile(tombstone, Array.Empty<ChunkDescriptor>());

            // Update Merkle prefix tree
            UpdateMerklePrefixes(normalizedPath);

            return Task.FromResult(true);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public Task<IReadOnlyList<FileMetadata>> GetAllFilesAsync(bool includeDeleted = false, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _lock.EnterReadLock();
        try
        {
            var query = _files.Values.Select(v => v.Metadata);
            if (!includeDeleted)
            {
                query = query.Where(m => !m.IsDeleted);
            }

            var list = query
                .OrderBy(m => m.RelativePath, StringComparer.OrdinalIgnoreCase)
                .ToList();

            return Task.FromResult<IReadOnlyList<FileMetadata>>(list);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public async Task<ChunkLocation?> GetChunkLocationAsync(string chunkHash, CancellationToken cancellationToken = default)
    {
        var locations = await GetChunkLocationsAsync(chunkHash, cancellationToken).ConfigureAwait(false);
        return locations.Count > 0 ? locations[0] : null;
    }

    public Task<IReadOnlyList<ChunkLocation>> GetChunkLocationsAsync(string chunkHash, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrWhiteSpace(chunkHash);

        string normalizedHash = chunkHash.Trim().ToLowerInvariant();
        if (normalizedHash.Length != 64)
        {
            throw new ArgumentException("Chunk hash must be a 64-character hexadecimal SHA-256 string.", nameof(chunkHash));
        }

        var list = new List<ChunkLocation>();
        _lock.EnterReadLock();
        try
        {
            foreach (var file in _files.Values)
            {
                if (file.Metadata.IsDeleted)
                    continue;

                foreach (var chunk in file.Chunks)
                {
                    if (string.Equals(chunk.HashHex, normalizedHash, StringComparison.OrdinalIgnoreCase))
                    {
                        list.Add(new ChunkLocation(file.Metadata.RelativePath, chunk.Offset, chunk.Length));
                    }
                }
            }

            return Task.FromResult<IReadOnlyList<ChunkLocation>>(list);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public Task<MerkleNode?> GetMerkleNodeAsync(string prefix, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        string normalizedPrefix = MerkleTreeHelper.NormalizePrefix(prefix);

        _lock.EnterReadLock();
        try
        {
            if (_merkleNodes.TryGetValue(normalizedPrefix, out var record))
            {
                return Task.FromResult<MerkleNode?>(new MerkleNode(normalizedPrefix, record.NodeHash, record.ChildCount, record.UpdatedUtc));
            }

            if (normalizedPrefix.Length == 0)
            {
                int activeCount = _files.Values.Count(v => !v.Metadata.IsDeleted);
                if (activeCount == 0)
                {
                    return Task.FromResult<MerkleNode?>(new MerkleNode(string.Empty, MerkleTreeHelper.EmptyNodeHash, 0, DateTimeOffset.UtcNow));
                }
            }

            return Task.FromResult<MerkleNode?>(null);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public Task<DirectoryDifference> GetDirectoryDifferenceAsync(string prefix, string remoteNodeHash, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        string normalizedPrefix = MerkleTreeHelper.NormalizePrefix(prefix);

        _lock.EnterReadLock();
        try
        {
            MerkleNode? localNode = null;
            if (_merkleNodes.TryGetValue(normalizedPrefix, out var record))
            {
                localNode = new MerkleNode(normalizedPrefix, record.NodeHash, record.ChildCount, record.UpdatedUtc);
            }
            else if (normalizedPrefix.Length == 0 && _files.Values.Count(v => !v.Metadata.IsDeleted) == 0)
            {
                localNode = new MerkleNode(string.Empty, MerkleTreeHelper.EmptyNodeHash, 0, DateTimeOffset.UtcNow);
            }

            bool areIdentical = localNode != null &&
                !string.IsNullOrWhiteSpace(remoteNodeHash) &&
                string.Equals(localNode.NodeHash, remoteNodeHash.Trim(), StringComparison.OrdinalIgnoreCase);

            if (areIdentical)
            {
                return Task.FromResult(new DirectoryDifference(
                    normalizedPrefix,
                    true,
                    localNode!.NodeHash,
                    remoteNodeHash,
                    Array.Empty<FileMetadata>(),
                    Array.Empty<MerkleNode>()));
            }

            var (directFiles, directSubdirs) = GetDirectChildren(normalizedPrefix);

            var diff = new DirectoryDifference(
                normalizedPrefix,
                false,
                localNode?.NodeHash,
                remoteNodeHash,
                directFiles.Select(f => f.Metadata).OrderBy(m => m.RelativePath, StringComparer.OrdinalIgnoreCase).ToList(),
                directSubdirs.Select(s => new MerkleNode(s.Prefix, s.NodeHash, s.ChildCount, s.UpdatedUtc)).OrderBy(m => m.Prefix, StringComparer.OrdinalIgnoreCase).ToList()
            );

            return Task.FromResult(diff);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    private void UpdateMerklePrefixes(string normalizedFilePath)
    {
        var prefixChain = MerkleTreeHelper.GetPrefixChain(normalizedFilePath);
        DateTimeOffset now = DateTimeOffset.UtcNow;

        foreach (string prefix in prefixChain)
        {
            var (directFiles, directSubdirs) = GetDirectChildren(prefix);

            int totalCount = directFiles.Count + directSubdirs.Sum(s => s.ChildCount);

            if (directFiles.Count == 0 && directSubdirs.Count == 0 && prefix.Length > 0)
            {
                _merkleNodes.TryRemove(prefix, out _);
            }
            else
            {
                string nodeHash = MerkleTreeHelper.ComputeNodeHash(
                    directFiles.Select(f => (Path.GetFileName(f.Metadata.RelativePath), f.Metadata.RootHash)),
                    directSubdirs.Select(s => (GetImmediateDirName(prefix, s.Prefix), s.NodeHash)));

                _merkleNodes[prefix] = new MerkleNodeRecord(nodeHash, totalCount, now);
            }
        }
    }

    private (List<StoredFile> Files, List<(string Prefix, string NodeHash, int ChildCount, DateTimeOffset UpdatedUtc)> Subdirs) GetDirectChildren(string prefix)
    {
        var directFiles = new List<StoredFile>();
        foreach (var file in _files.Values)
        {
            if (file.Metadata.IsDeleted)
                continue;

            string path = file.Metadata.RelativePath;
            if (prefix.Length == 0)
            {
                if (!path.Contains('/'))
                {
                    directFiles.Add(file);
                }
            }
            else
            {
                if (path.StartsWith(prefix + "/", StringComparison.OrdinalIgnoreCase))
                {
                    string remainder = path[(prefix.Length + 1)..];
                    if (!remainder.Contains('/'))
                    {
                        directFiles.Add(file);
                    }
                }
            }
        }

        var directSubdirs = new List<(string Prefix, string NodeHash, int ChildCount, DateTimeOffset UpdatedUtc)>();
        foreach (var (p, record) in _merkleNodes)
        {
            if (p.Length == 0 || string.Equals(p, prefix, StringComparison.OrdinalIgnoreCase))
                continue;

            if (prefix.Length == 0)
            {
                if (!p.Contains('/'))
                {
                    directSubdirs.Add((p, record.NodeHash, record.ChildCount, record.UpdatedUtc));
                }
            }
            else
            {
                if (p.StartsWith(prefix + "/", StringComparison.OrdinalIgnoreCase))
                {
                    string remainder = p[(prefix.Length + 1)..];
                    if (!remainder.Contains('/'))
                    {
                        directSubdirs.Add((p, record.NodeHash, record.ChildCount, record.UpdatedUtc));
                    }
                }
            }
        }

        return (directFiles, directSubdirs);
    }

    private static string GetImmediateDirName(string parentPrefix, string subPrefix)
    {
        if (parentPrefix.Length == 0)
        {
            return subPrefix;
        }

        return subPrefix[(parentPrefix.Length + 1)..];
    }

    private static string NormalizeAndValidatePath(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            throw new ArgumentException("Relative path cannot be null, empty, or whitespace.", nameof(path));
        }

        string normalized = path.Trim().Replace('\\', '/').TrimStart('/');
        if (string.IsNullOrWhiteSpace(normalized))
        {
            throw new ArgumentException("Relative path cannot resolve to an empty path.", nameof(path));
        }

        string[] segments = normalized.Split('/');
        foreach (string seg in segments)
        {
            if (seg is "." or "..")
            {
                throw new ArgumentException($"Relative path '{path}' contains invalid traversal component '{seg}'.", nameof(path));
            }
        }

        return normalized;
    }

    private static void ValidateFileMetadata(FileMetadata file, IReadOnlyList<ChunkDescriptor> chunks)
    {
        if (file.SizeBytes < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(file), "File size must be non-negative.");
        }

        if (string.IsNullOrWhiteSpace(file.RootHash) || file.RootHash.Trim().Length != 64)
        {
            throw new ArgumentException("Root hash must be a 64-character lowercase hex SHA-256 string.", nameof(file));
        }

        if (chunks.Count == 0)
        {
            if (file.SizeBytes != 0)
            {
                throw new ArgumentException("File with 0 chunks must have SizeBytes == 0.", nameof(chunks));
            }
            return;
        }

        long cumulativeOffset = 0;
        for (int i = 0; i < chunks.Count; i++)
        {
            var chunk = chunks[i] ?? throw new ArgumentException($"Chunk at index {i} cannot be null.", nameof(chunks));
            if (chunk.Index != i)
            {
                throw new ArgumentException($"Chunk {i} has non-monotonic index {chunk.Index}.", nameof(chunks));
            }
            if (chunk.Offset != cumulativeOffset)
            {
                throw new ArgumentException($"Chunk {i} has non-contiguous offset {chunk.Offset}. Expected {cumulativeOffset}.", nameof(chunks));
            }
            cumulativeOffset += chunk.Length;
        }

        if (cumulativeOffset != file.SizeBytes)
        {
            throw new ArgumentException($"Sum of chunk lengths ({cumulativeOffset}) does not equal file size ({file.SizeBytes}).", nameof(chunks));
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _lock.Dispose();
        }
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
