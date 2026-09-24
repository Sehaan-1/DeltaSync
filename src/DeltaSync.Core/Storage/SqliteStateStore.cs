using System.Data.Common;
using System.Security.Cryptography;
using System.Text.Json;
using DeltaSync.Core.Causality;
using DeltaSync.Core.Chunking;
using DeltaSync.Core.Models;
using Microsoft.Data.Sqlite;

namespace DeltaSync.Core.Storage;

/// <summary>
/// High-performance embedded SQLite state store providing atomic file metadata ingestion,
/// FastCDC chunk inverted index resolution, reference-counted chunk lifecycle management,
/// and fast delta manifest probing.
/// Honors ADR-0003 and Invariants I1, I2, I3, I4.
/// </summary>
public sealed class SqliteStateStore : ISqliteStateStore
{
    private readonly ISqliteConnectionFactory _connectionFactory;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    // H-05 fix: volatile ensures CPU cache coherence so any thread sees the updated value
    // immediately without acquiring a lock. Avoids stale-false reads on multi-core systems.
    private volatile bool _disposed;

    public SqliteStateStore(ISqliteConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
    }

    public SqliteStateStore(string databasePath)
        : this(new SqliteConnectionFactory(databasePath))
    {
    }

    public async Task UpsertFileAsync(FileMetadata file, IReadOnlyList<ChunkDescriptor> chunks, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(chunks);

        string normalizedPath = NormalizeAndValidatePath(file.RelativePath);
        ValidateFileMetadata(file, chunks);

        long nowTicks = DateTime.UtcNow.Ticks;
        string vectorClockJson = JsonSerializer.Serialize(file.Clock ?? VectorClock.Empty);

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var tx = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

            long fileId;
            int existingVersion = 0;

            // Check if file exists
            await using (var checkCmd = connection.CreateCommand())
            {
                checkCmd.Transaction = (SqliteTransaction)tx;
                checkCmd.CommandText = "SELECT file_id, version FROM files WHERE relative_path = $path;";
                checkCmd.Parameters.AddWithValue("$path", normalizedPath);
                await using var reader = await checkCmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    fileId = reader.GetInt64(0);
                    existingVersion = reader.GetInt32(1);
                }
                else
                {
                    fileId = -1;
                }
            }

            if (fileId != -1)
            {
                // Decrement ref_count for all chunks currently mapped to this file
                await using (var decCmd = connection.CreateCommand())
                {
                    decCmd.Transaction = (SqliteTransaction)tx;
                    decCmd.CommandText = @"
                        UPDATE chunks SET ref_count = ref_count - 1 
                        WHERE chunk_hash IN (SELECT chunk_hash FROM file_chunks WHERE file_id = $fid);";
                    decCmd.Parameters.AddWithValue("$fid", fileId);
                    await decCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                // Delete existing file_chunks mappings first so foreign key constraints on chunks are cleared
                await using (var delFcCmd = connection.CreateCommand())
                {
                    delFcCmd.Transaction = (SqliteTransaction)tx;
                    delFcCmd.CommandText = "DELETE FROM file_chunks WHERE file_id = $fid;";
                    delFcCmd.Parameters.AddWithValue("$fid", fileId);
                    await delFcCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                // Delete orphaned chunks whose ref_count reached 0 or less
                await using (var pruneCmd = connection.CreateCommand())
                {
                    pruneCmd.Transaction = (SqliteTransaction)tx;
                    pruneCmd.CommandText = "DELETE FROM chunks WHERE ref_count <= 0;";
                    await pruneCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                // Update file record
                await using (var updateCmd = connection.CreateCommand())
                {
                    updateCmd.Transaction = (SqliteTransaction)tx;
                    updateCmd.CommandText = @"
                        UPDATE files 
                        SET size_bytes = $size, root_hash = $hash, modified_utc_ticks = $mod,
                            vector_clock_json = $vc, is_deleted = $del, version = version + 1, updated_utc_ticks = $now
                        WHERE file_id = $fid;";
                    updateCmd.Parameters.AddWithValue("$size", file.SizeBytes);
                    updateCmd.Parameters.AddWithValue("$hash", file.RootHash);
                    updateCmd.Parameters.AddWithValue("$mod", file.ModifiedUtc.UtcTicks);
                    updateCmd.Parameters.AddWithValue("$vc", vectorClockJson);
                    updateCmd.Parameters.AddWithValue("$del", file.IsDeleted ? 1 : 0);
                    updateCmd.Parameters.AddWithValue("$now", nowTicks);
                    updateCmd.Parameters.AddWithValue("$fid", fileId);
                    await updateCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            else
            {
                // Insert new file record
                await using (var insertCmd = connection.CreateCommand())
                {
                    insertCmd.Transaction = (SqliteTransaction)tx;
                    insertCmd.CommandText = @"
                        INSERT INTO files (relative_path, size_bytes, root_hash, modified_utc_ticks, vector_clock_json, is_deleted, version, updated_utc_ticks)
                        VALUES ($path, $size, $hash, $mod, $vc, $del, $ver, $now);
                        SELECT last_insert_rowid();";
                    insertCmd.Parameters.AddWithValue("$path", normalizedPath);
                    insertCmd.Parameters.AddWithValue("$size", file.SizeBytes);
                    insertCmd.Parameters.AddWithValue("$hash", file.RootHash);
                    insertCmd.Parameters.AddWithValue("$mod", file.ModifiedUtc.UtcTicks);
                    insertCmd.Parameters.AddWithValue("$vc", vectorClockJson);
                    insertCmd.Parameters.AddWithValue("$del", file.IsDeleted ? 1 : 0);
                    insertCmd.Parameters.AddWithValue("$ver", file.Version);
                    insertCmd.Parameters.AddWithValue("$now", nowTicks);
                    fileId = Convert.ToInt64(await insertCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
                }
            }

            // Insert chunks and file_chunks mappings
            if (chunks.Count > 0)
            {
                await using var chunkCmd = connection.CreateCommand();
                chunkCmd.Transaction = (SqliteTransaction)tx;
                chunkCmd.CommandText = @"
                    INSERT INTO chunks (chunk_hash, length, ref_count, first_seen_utc_ticks)
                    VALUES ($hash, $len, 1, $now)
                    ON CONFLICT(chunk_hash) DO UPDATE SET ref_count = ref_count + 1;";
                var pChunkHash = chunkCmd.Parameters.Add("$hash", SqliteType.Text);
                var pChunkLen = chunkCmd.Parameters.Add("$len", SqliteType.Integer);
                var pChunkNow = chunkCmd.Parameters.Add("$now", SqliteType.Integer);
                pChunkNow.Value = nowTicks;

                await using var fcCmd = connection.CreateCommand();
                fcCmd.Transaction = (SqliteTransaction)tx;
                fcCmd.CommandText = @"
                    INSERT INTO file_chunks (file_id, chunk_index, chunk_hash, offset, length)
                    VALUES ($fid, $idx, $hash, $off, $len);";
                fcCmd.Parameters.AddWithValue("$fid", fileId);
                var pFcIdx = fcCmd.Parameters.Add("$idx", SqliteType.Integer);
                var pFcHash = fcCmd.Parameters.Add("$hash", SqliteType.Text);
                var pFcOff = fcCmd.Parameters.Add("$off", SqliteType.Integer);
                var pFcLen = fcCmd.Parameters.Add("$len", SqliteType.Integer);

                for (int i = 0; i < chunks.Count; i++)
                {
                    var chunk = chunks[i];
                    string chunkHash = chunk.HashHex;

                    pChunkHash.Value = chunkHash;
                    pChunkLen.Value = chunk.Length;
                    await chunkCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

                    pFcIdx.Value = chunk.Index;
                    pFcHash.Value = chunkHash;
                    pFcOff.Value = chunk.Offset;
                    pFcLen.Value = chunk.Length;
                    await fcCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            // Trigger incremental Merkle prefix update for directory Pf
            await UpdateMerklePrefixesAsync(connection, (SqliteTransaction)tx, normalizedPath, cancellationToken).ConfigureAwait(false);

            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task<FileMetadata?> GetFileAsync(string relativePath, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        string normalizedPath = NormalizeAndValidatePath(relativePath);

        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            SELECT relative_path, size_bytes, root_hash, modified_utc_ticks, vector_clock_json, is_deleted, version, updated_utc_ticks
            FROM files
            WHERE relative_path = $path;";
        cmd.Parameters.AddWithValue("$path", normalizedPath);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            return null;
        }

        return ReadFileMetadata(reader);
    }

    public async Task<IReadOnlyList<ChunkDescriptor>> GetFileChunksAsync(string relativePath, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        string normalizedPath = NormalizeAndValidatePath(relativePath);

        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            SELECT fc.chunk_index, fc.chunk_hash, fc.offset, fc.length
            FROM file_chunks fc
            INNER JOIN files f ON fc.file_id = f.file_id
            WHERE f.relative_path = $path
            ORDER BY fc.chunk_index ASC;";
        cmd.Parameters.AddWithValue("$path", normalizedPath);

        var list = new List<ChunkDescriptor>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            int index = reader.GetInt32(0);
            string hashHex = reader.GetString(1);
            long offset = reader.GetInt64(2);
            int length = reader.GetInt32(3);

            byte[] hashBytes = Convert.FromHexString(hashHex);
            list.Add(new ChunkDescriptor(index, offset, length, hashBytes));
        }

        return list;
    }

    public async Task<ChunkProbeResult> ProbeChunksAsync(IEnumerable<string> chunkHashes, CancellationToken cancellationToken = default)
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
            return new ChunkProbeResult(new HashSet<string>(), new HashSet<string>());
        }

        var localHashes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        // For small probe sets (<= 16), perform a direct read query to avoid temp table and transaction overhead for concurrent readers.
        if (uniqueHashes.Count <= 16)
        {
            await using var selectDirectCmd = connection.CreateCommand();
            if (uniqueHashes.Count == 1)
            {
                string singleHash = uniqueHashes.First();
                selectDirectCmd.CommandText = "SELECT chunk_hash FROM chunks WHERE chunk_hash = $h0;";
                selectDirectCmd.Parameters.AddWithValue("$h0", singleHash);
            }
            else
            {
                var paramNames = new string[uniqueHashes.Count];
                int idx = 0;
                foreach (var h in uniqueHashes)
                {
                    string pName = "$h" + idx;
                    paramNames[idx++] = pName;
                    selectDirectCmd.Parameters.AddWithValue(pName, h);
                }
                selectDirectCmd.CommandText = $"SELECT chunk_hash FROM chunks WHERE chunk_hash IN ({string.Join(',', paramNames)});";
            }

            await using var reader = await selectDirectCmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                localHashes.Add(reader.GetString(0));
            }

            var missingDirect = new HashSet<string>(uniqueHashes.Except(localHashes, StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);
            return new ChunkProbeResult(localHashes, missingDirect);
        }

        // M-02 fix: for large probe sets (> 16), use a session-scoped temp table populated in a single transaction
        // instead of a massive dynamic IN clause. SQLite prepares the INSERT and SELECT once.
        await using (var createTmp = connection.CreateCommand())
        {
            createTmp.CommandText = "CREATE TEMP TABLE IF NOT EXISTS _probe_hashes (hash TEXT PRIMARY KEY NOT NULL);";
            await createTmp.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        try
        {
            // Populate the temp table with all hashes to probe within an explicit single transaction
            await using var tx = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using var insertCmd = connection.CreateCommand();
            insertCmd.Transaction = (SqliteTransaction)tx;
            insertCmd.CommandText = "INSERT OR IGNORE INTO _probe_hashes (hash) VALUES ($h);";
            var pInsert = insertCmd.Parameters.Add("$h", SqliteType.Text);

            foreach (var hash in uniqueHashes)
            {
                cancellationToken.ThrowIfCancellationRequested();
                pInsert.Value = hash;
                await insertCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);

            // Single JOIN query to find which hashes already exist in the chunk store
            await using var selectCmd = connection.CreateCommand();
            selectCmd.CommandText = @"
                SELECT c.chunk_hash
                FROM chunks c
                INNER JOIN _probe_hashes p ON c.chunk_hash = p.hash;";

            await using var reader = await selectCmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                localHashes.Add(reader.GetString(0));
            }
        }
        finally
        {
            // Clean up temp table so it doesn't persist across calls on a pooled connection
            try
            {
                await using var dropTmp = connection.CreateCommand();
                dropTmp.CommandText = "DROP TABLE IF EXISTS _probe_hashes;";
                await dropTmp.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch { /* best-effort cleanup */ }
        }

        var missingHashes = new HashSet<string>(uniqueHashes.Except(localHashes, StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);
        return new ChunkProbeResult(localHashes, missingHashes);
    }

    public async Task<bool> DeleteFileAsync(string relativePath, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        string normalizedPath = NormalizeAndValidatePath(relativePath);

        long nowTicks = DateTime.UtcNow.Ticks;

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var tx = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

            long fileId = -1;
            await using (var checkCmd = connection.CreateCommand())
            {
                checkCmd.Transaction = (SqliteTransaction)tx;
                checkCmd.CommandText = "SELECT file_id FROM files WHERE relative_path = $path AND is_deleted = 0;";
                checkCmd.Parameters.AddWithValue("$path", normalizedPath);
                var result = await checkCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                if (result != null && result != DBNull.Value)
                {
                    fileId = Convert.ToInt64(result);
                }
            }

            if (fileId == -1)
            {
                return false;
            }

            // Decrement ref_count for mapped chunks
            await using (var decCmd = connection.CreateCommand())
            {
                decCmd.Transaction = (SqliteTransaction)tx;
                decCmd.CommandText = @"
                    UPDATE chunks SET ref_count = ref_count - 1 
                    WHERE chunk_hash IN (SELECT chunk_hash FROM file_chunks WHERE file_id = $fid);";
                decCmd.Parameters.AddWithValue("$fid", fileId);
                await decCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            // Delete file_chunks mappings first so foreign key constraints on chunks are cleared
            await using (var delFcCmd = connection.CreateCommand())
            {
                delFcCmd.Transaction = (SqliteTransaction)tx;
                delFcCmd.CommandText = "DELETE FROM file_chunks WHERE file_id = $fid;";
                delFcCmd.Parameters.AddWithValue("$fid", fileId);
                await delFcCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            // Purge unreferenced chunks
            await using (var pruneCmd = connection.CreateCommand())
            {
                pruneCmd.Transaction = (SqliteTransaction)tx;
                pruneCmd.CommandText = "DELETE FROM chunks WHERE ref_count <= 0;";
                await pruneCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            // Mark file deleted (tombstone)
            await using (var tombstoneCmd = connection.CreateCommand())
            {
                tombstoneCmd.Transaction = (SqliteTransaction)tx;
                tombstoneCmd.CommandText = @"
                    UPDATE files 
                    SET is_deleted = 1, size_bytes = 0, version = version + 1, updated_utc_ticks = $now 
                    WHERE file_id = $fid;";
                tombstoneCmd.Parameters.AddWithValue("$now", nowTicks);
                tombstoneCmd.Parameters.AddWithValue("$fid", fileId);
                await tombstoneCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            // Trigger incremental Merkle prefix update for directory Pf
            await UpdateMerklePrefixesAsync(connection, (SqliteTransaction)tx, normalizedPath, cancellationToken).ConfigureAwait(false);

            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task<IReadOnlyList<FileMetadata>> GetAllFilesAsync(bool includeDeleted = false, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();

        cmd.CommandText = includeDeleted
            ? @"SELECT relative_path, size_bytes, root_hash, modified_utc_ticks, vector_clock_json, is_deleted, version, updated_utc_ticks 
               FROM files ORDER BY relative_path COLLATE NOCASE ASC;"
            : @"SELECT relative_path, size_bytes, root_hash, modified_utc_ticks, vector_clock_json, is_deleted, version, updated_utc_ticks 
               FROM files WHERE is_deleted = 0 ORDER BY relative_path COLLATE NOCASE ASC;";

        var list = new List<FileMetadata>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            list.Add(ReadFileMetadata(reader));
        }

        return list;
    }

    public async Task<ChunkLocation?> GetChunkLocationAsync(string chunkHash, CancellationToken cancellationToken = default)
    {
        var locations = await GetChunkLocationsAsync(chunkHash, cancellationToken).ConfigureAwait(false);
        return locations.Count > 0 ? locations[0] : null;
    }

    public async Task<IReadOnlyList<ChunkLocation>> GetChunkLocationsAsync(string chunkHash, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrWhiteSpace(chunkHash);

        string normalizedHash = chunkHash.Trim().ToLowerInvariant();
        if (normalizedHash.Length != 64)
        {
            throw new ArgumentException("Chunk hash must be a 64-character hexadecimal SHA-256 string.", nameof(chunkHash));
        }

        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            SELECT f.relative_path, fc.offset, fc.length
            FROM file_chunks fc
            INNER JOIN files f ON fc.file_id = f.file_id
            WHERE fc.chunk_hash = $hash AND f.is_deleted = 0;";
        cmd.Parameters.AddWithValue("$hash", normalizedHash);

        var list = new List<ChunkLocation>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            string relativePath = reader.GetString(0);
            long offset = reader.GetInt64(1);
            int length = reader.GetInt32(2);
            list.Add(new ChunkLocation(relativePath, offset, length));
        }

        return list;
    }

    public async Task<MerkleNode?> GetMerkleNodeAsync(string prefix, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        string normalizedPrefix = MerkleTreeHelper.NormalizePrefix(prefix);

        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
            SELECT prefix, node_hash, child_count, updated_utc_ticks
            FROM merkle_nodes
            WHERE prefix = $prefix;";
        cmd.Parameters.AddWithValue("$prefix", normalizedPrefix);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            string pref = reader.GetString(0);
            string hash = reader.GetString(1);
            int count = reader.GetInt32(2);
            long ticks = reader.GetInt64(3);
            return new MerkleNode(pref, hash, count, new DateTimeOffset(ticks, TimeSpan.Zero));
        }

        // If root node """" was queried and table has 0 active files, return canonical empty root node
        if (normalizedPrefix.Length == 0)
        {
            await using var countCmd = connection.CreateCommand();
            countCmd.CommandText = "SELECT COUNT(*) FROM files WHERE is_deleted = 0;";
            long fileCount = Convert.ToInt64(await countCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false));
            if (fileCount == 0)
            {
                return new MerkleNode(string.Empty, MerkleTreeHelper.EmptyNodeHash, 0, DateTimeOffset.UtcNow);
            }
        }

        return null;
    }

    public async Task<DirectoryDifference> GetDirectoryDifferenceAsync(string prefix, string remoteNodeHash, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        string normalizedPrefix = MerkleTreeHelper.NormalizePrefix(prefix);

        var localNode = await GetMerkleNodeAsync(normalizedPrefix, cancellationToken).ConfigureAwait(false);

        bool areIdentical = localNode != null &&
            !string.IsNullOrWhiteSpace(remoteNodeHash) &&
            string.Equals(localNode.NodeHash, remoteNodeHash.Trim(), StringComparison.OrdinalIgnoreCase);

        if (areIdentical)
        {
            return new DirectoryDifference(
                normalizedPrefix,
                true,
                localNode!.NodeHash,
                remoteNodeHash,
                Array.Empty<FileMetadata>(),
                Array.Empty<MerkleNode>());
        }

        // Query direct child files
        var directFiles = new List<FileMetadata>();
        await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (var fileCmd = connection.CreateCommand())
        {
            if (normalizedPrefix.Length == 0)
            {
                fileCmd.CommandText = @"
                    SELECT relative_path, size_bytes, root_hash, modified_utc_ticks, vector_clock_json, is_deleted, version, updated_utc_ticks
                    FROM files
                    WHERE is_deleted = 0 AND instr(relative_path, '/') = 0
                    ORDER BY relative_path COLLATE NOCASE ASC;";
            }
            else
            {
                fileCmd.CommandText = @"
                    SELECT relative_path, size_bytes, root_hash, modified_utc_ticks, vector_clock_json, is_deleted, version, updated_utc_ticks
                    FROM files
                    WHERE is_deleted = 0
                      AND relative_path LIKE $prefixMatch ESCAPE '\'
                      AND instr(substr(relative_path, $prefixLen + 2), '/') = 0
                    ORDER BY relative_path COLLATE NOCASE ASC;";
                fileCmd.Parameters.AddWithValue("$prefixMatch", EscapeLike(normalizedPrefix) + "/%");
                fileCmd.Parameters.AddWithValue("$prefixLen", normalizedPrefix.Length);
            }

            await using var reader = await fileCmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                directFiles.Add(ReadFileMetadata(reader));
            }
        }

        // Query immediate child subdirectories from merkle_nodes
        var directSubdirs = new List<MerkleNode>();
        await using (var dirCmd = connection.CreateCommand())
        {
            if (normalizedPrefix.Length == 0)
            {
                dirCmd.CommandText = @"
                    SELECT prefix, node_hash, child_count, updated_utc_ticks
                    FROM merkle_nodes
                    WHERE prefix != '' AND instr(prefix, '/') = 0
                    ORDER BY prefix COLLATE NOCASE ASC;";
            }
            else
            {
                dirCmd.CommandText = @"
                    SELECT prefix, node_hash, child_count, updated_utc_ticks
                    FROM merkle_nodes
                    WHERE prefix != $self
                      AND prefix LIKE $prefixMatch ESCAPE '\'
                      AND instr(substr(prefix, $prefixLen + 2), '/') = 0
                    ORDER BY prefix COLLATE NOCASE ASC;";
                dirCmd.Parameters.AddWithValue("$self", normalizedPrefix);
                dirCmd.Parameters.AddWithValue("$prefixMatch", EscapeLike(normalizedPrefix) + "/%");
                dirCmd.Parameters.AddWithValue("$prefixLen", normalizedPrefix.Length);
            }

            await using var reader = await dirCmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                string p = reader.GetString(0);
                string h = reader.GetString(1);
                int c = reader.GetInt32(2);
                long ticks = reader.GetInt64(3);
                directSubdirs.Add(new MerkleNode(p, h, c, new DateTimeOffset(ticks, TimeSpan.Zero)));
            }
        }

        return new DirectoryDifference(
            normalizedPrefix,
            false,
            localNode?.NodeHash,
            remoteNodeHash,
            directFiles,
            directSubdirs);
    }

    public async Task RebuildMerkleTreeAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        await _writeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await using var connection = await _connectionFactory.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await using var tx = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

            await using (var clearCmd = connection.CreateCommand())
            {
                clearCmd.Transaction = (SqliteTransaction)tx;
                clearCmd.CommandText = "DELETE FROM merkle_nodes;";
                await clearCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            var paths = new List<string>();
            await using (var listCmd = connection.CreateCommand())
            {
                listCmd.Transaction = (SqliteTransaction)tx;
                listCmd.CommandText = "SELECT relative_path FROM files WHERE is_deleted = 0 ORDER BY length(relative_path) DESC;";
                await using var reader = await listCmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    paths.Add(reader.GetString(0));
                }
            }

            foreach (string path in paths)
            {
                await UpdateMerklePrefixesAsync(connection, (SqliteTransaction)tx, path, cancellationToken).ConfigureAwait(false);
            }

            if (paths.Count == 0)
            {
                await using var rootCmd = connection.CreateCommand();
                rootCmd.Transaction = (SqliteTransaction)tx;
                rootCmd.CommandText = @"
                    INSERT INTO merkle_nodes (prefix, node_hash, child_count, updated_utc_ticks)
                    VALUES ('', $hash, 0, $now)
                    ON CONFLICT(prefix) DO NOTHING;";
                rootCmd.Parameters.AddWithValue("$hash", MerkleTreeHelper.EmptyNodeHash);
                rootCmd.Parameters.AddWithValue("$now", DateTime.UtcNow.Ticks);
                await rootCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private static async Task UpdateMerklePrefixesAsync(
        SqliteConnection connection,
        SqliteTransaction tx,
        string normalizedFilePath,
        CancellationToken cancellationToken)
    {
        var prefixChain = MerkleTreeHelper.GetPrefixChain(normalizedFilePath);
        long nowTicks = DateTime.UtcNow.Ticks;

        foreach (string prefix in prefixChain)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // 1. Direct active files under prefix
            var directFiles = new List<(string Name, string RootHash)>();
            await using (var fileCmd = connection.CreateCommand())
            {
                fileCmd.Transaction = tx;
                if (prefix.Length == 0)
                {
                    fileCmd.CommandText = @"
                        SELECT relative_path, root_hash 
                        FROM files 
                        WHERE is_deleted = 0 AND instr(relative_path, '/') = 0 
                        ORDER BY relative_path COLLATE NOCASE ASC;";
                }
                else
                {
                    fileCmd.CommandText = @"
                        SELECT relative_path, root_hash 
                        FROM files 
                        WHERE is_deleted = 0 
                          AND relative_path LIKE $prefixMatch ESCAPE '\'
                          AND instr(substr(relative_path, $prefixLen + 2), '/') = 0
                        ORDER BY relative_path COLLATE NOCASE ASC;";
                    fileCmd.Parameters.AddWithValue("$prefixMatch", EscapeLike(prefix) + "/%");
                    fileCmd.Parameters.AddWithValue("$prefixLen", prefix.Length);
                }

                await using var reader = await fileCmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    string path = reader.GetString(0);
                    string hash = reader.GetString(1);
                    string fileName = prefix.Length == 0 ? path : path[(prefix.Length + 1)..];
                    directFiles.Add((fileName, hash));
                }
            }

            // 2. Immediate child subdirectories from merkle_nodes
            var directSubdirs = new List<(string Name, string NodeHash, int ChildCount)>();
            await using (var dirCmd = connection.CreateCommand())
            {
                dirCmd.Transaction = tx;
                if (prefix.Length == 0)
                {
                    dirCmd.CommandText = @"
                        SELECT prefix, node_hash, child_count 
                        FROM merkle_nodes 
                        WHERE prefix != '' AND instr(prefix, '/') = 0 
                        ORDER BY prefix COLLATE NOCASE ASC;";
                }
                else
                {
                    dirCmd.CommandText = @"
                        SELECT prefix, node_hash, child_count 
                        FROM merkle_nodes 
                        WHERE prefix != $self 
                          AND prefix LIKE $prefixMatch ESCAPE '\'
                          AND instr(substr(prefix, $prefixLen + 2), '/') = 0 
                        ORDER BY prefix COLLATE NOCASE ASC;";
                    dirCmd.Parameters.AddWithValue("$self", prefix);
                    dirCmd.Parameters.AddWithValue("$prefixMatch", EscapeLike(prefix) + "/%");
                    dirCmd.Parameters.AddWithValue("$prefixLen", prefix.Length);
                }

                await using var reader = await dirCmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    string subPrefix = reader.GetString(0);
                    string hash = reader.GetString(1);
                    int count = reader.GetInt32(2);
                    string dirName = prefix.Length == 0 ? subPrefix : subPrefix[(prefix.Length + 1)..];
                    directSubdirs.Add((dirName, hash, count));
                }
            }

            int totalChildCount = directFiles.Count + directSubdirs.Sum(s => s.ChildCount);

            if (directFiles.Count == 0 && directSubdirs.Count == 0 && prefix.Length > 0)
            {
                await using var delCmd = connection.CreateCommand();
                delCmd.Transaction = tx;
                delCmd.CommandText = "DELETE FROM merkle_nodes WHERE prefix = $prefix;";
                delCmd.Parameters.AddWithValue("$prefix", prefix);
                await delCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                string nodeHash = MerkleTreeHelper.ComputeNodeHash(
                    directFiles,
                    directSubdirs.Select(s => (s.Name, s.NodeHash)));

                await using var upsertCmd = connection.CreateCommand();
                upsertCmd.Transaction = tx;
                upsertCmd.CommandText = @"
                    INSERT INTO merkle_nodes (prefix, node_hash, child_count, updated_utc_ticks)
                    VALUES ($prefix, $hash, $count, $now)
                    ON CONFLICT(prefix) DO UPDATE SET node_hash = $hash, child_count = $count, updated_utc_ticks = $now;";
                upsertCmd.Parameters.AddWithValue("$prefix", prefix);
                upsertCmd.Parameters.AddWithValue("$hash", nodeHash);
                upsertCmd.Parameters.AddWithValue("$count", totalChildCount);
                upsertCmd.Parameters.AddWithValue("$now", nowTicks);
                await upsertCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static string EscapeLike(string value)
    {
        return value.Replace("\\", "\\\\").Replace("%", "\\%").Replace("_", "\\_");
    }

    private static FileMetadata ReadFileMetadata(DbDataReader reader)
    {
        string relativePath = reader.GetString(0);
        long sizeBytes = reader.GetInt64(1);
        string rootHash = reader.GetString(2);
        long modifiedTicks = reader.GetInt64(3);
        string vcJson = reader.GetString(4);
        bool isDeleted = reader.GetInt32(5) == 1;
        int version = reader.GetInt32(6);
        long updatedTicks = reader.GetInt64(7);

        VectorClock clock;
        try
        {
            clock = JsonSerializer.Deserialize<VectorClock>(vcJson) ?? VectorClock.Empty;
        }
        catch
        {
            clock = VectorClock.Empty;
        }

        return new FileMetadata(
            relativePath,
            sizeBytes,
            rootHash,
            new DateTimeOffset(modifiedTicks, TimeSpan.Zero),
            clock,
            isDeleted,
            version,
            new DateTimeOffset(updatedTicks, TimeSpan.Zero)
        );
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
            _writeLock.Dispose();
        }
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }
}
