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
    private bool _disposed;

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

        // Parameterized batch queries (500 hashes per batch for optimal latency)
        const int batchSize = 500;
        var hashList = uniqueHashes.ToList();

        for (int i = 0; i < hashList.Count; i += batchSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            int currentBatchCount = Math.Min(batchSize, hashList.Count - i);

            await using var cmd = connection.CreateCommand();
            var parameters = new string[currentBatchCount];
            for (int j = 0; j < currentBatchCount; j++)
            {
                string paramName = $"$h{j}";
                parameters[j] = paramName;
                cmd.Parameters.AddWithValue(paramName, hashList[i + j]);
            }

            cmd.CommandText = $"SELECT chunk_hash FROM chunks WHERE chunk_hash IN ({string.Join(',', parameters)});";

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                localHashes.Add(reader.GetString(0));
            }
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
            WHERE fc.chunk_hash = $hash AND f.is_deleted = 0
            LIMIT 1;";
        cmd.Parameters.AddWithValue("$hash", normalizedHash);

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            string relativePath = reader.GetString(0);
            long offset = reader.GetInt64(1);
            int length = reader.GetInt32(2);
            return new ChunkLocation(relativePath, offset, length);
        }

        return null;
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
