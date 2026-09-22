using Microsoft.Win32.SafeHandles;

namespace DeltaSync.Core.Chunking;

/// <summary>
/// Provider contract for retrieving locally cached or existing file chunks by SHA-256 hash.
/// </summary>
public interface ILocalChunkProvider
{
    /// <summary>
    /// Synchronously attempts to retrieve a chunk payload by lowercase hex SHA-256 hash.
    /// </summary>
    bool TryGetChunk(string hashHex, out ReadOnlyMemory<byte> chunkData);

    /// <summary>
    /// Asynchronously attempts to retrieve a chunk payload by lowercase hex SHA-256 hash.
    /// </summary>
    ValueTask<ReadOnlyMemory<byte>?> GetChunkAsync(string hashHex, CancellationToken cancellationToken = default);
}

/// <summary>
/// Source contract for streaming missing chunks from a remote peer.
/// </summary>
public interface IRemoteChunkSource
{
    /// <summary>
    /// Fetches a missing chunk from the remote peer stream.
    /// </summary>
    ValueTask<ReadOnlyMemory<byte>> FetchChunkAsync(ChunkDescriptor chunk, CancellationToken cancellationToken = default);
}

/// <summary>
/// In-memory dictionary-backed implementation of ILocalChunkProvider for testing and ephemeral caches.
/// </summary>
public sealed class MemoryChunkProvider : ILocalChunkProvider
{
    private readonly Dictionary<string, byte[]> _chunks;

    public MemoryChunkProvider(IDictionary<string, byte[]>? chunks = null)
    {
        _chunks = chunks != null
            ? new Dictionary<string, byte[]>(chunks, StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);
    }

    public void AddChunk(string hashHex, byte[] data)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(hashHex);
        ArgumentNullException.ThrowIfNull(data);
        _chunks[hashHex.ToLowerInvariant()] = data;
    }

    public bool TryGetChunk(string hashHex, out ReadOnlyMemory<byte> chunkData)
    {
        if (_chunks.TryGetValue(hashHex, out var bytes))
        {
            chunkData = bytes;
            return true;
        }

        chunkData = default;
        return false;
    }

    public ValueTask<ReadOnlyMemory<byte>?> GetChunkAsync(string hashHex, CancellationToken cancellationToken = default)
    {
        if (TryGetChunk(hashHex, out var memory))
        {
            return new ValueTask<ReadOnlyMemory<byte>?>(memory);
        }

        return new ValueTask<ReadOnlyMemory<byte>?>((ReadOnlyMemory<byte>?)null);
    }
}

/// <summary>
/// Zero-copy, file-backed chunk provider that reads chunk slices on demand from an existing file on disk
/// using lock-free RandomAccess primitives in .NET 8.
/// </summary>
public sealed class FileChunkProvider : ILocalChunkProvider, IDisposable
{
    private readonly SafeFileHandle _fileHandle;
    private readonly Dictionary<string, ChunkDescriptor> _chunksByHash;
    private bool _disposed;

    public FileChunkProvider(string filePath, FileManifest manifest)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        ArgumentNullException.ThrowIfNull(manifest);

        if (!File.Exists(filePath))
            throw new FileNotFoundException($"Source file not found at: {filePath}", filePath);

        _fileHandle = File.OpenHandle(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);
        _chunksByHash = new Dictionary<string, ChunkDescriptor>(manifest.Chunks.Count, StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < manifest.Chunks.Count; i++)
        {
            var chunk = manifest.Chunks[i];
            _chunksByHash[chunk.HashHex] = chunk;
        }
    }

    public bool TryGetChunk(string hashHex, out ReadOnlyMemory<byte> chunkData)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_chunksByHash.TryGetValue(hashHex, out var descriptor))
        {
            chunkData = default;
            return false;
        }

        byte[] buffer = new byte[descriptor.Length];
        int bytesRead = RandomAccess.Read(_fileHandle, buffer, descriptor.Offset);
        if (bytesRead != descriptor.Length)
        {
            throw new IOException($"Could not read full chunk from file at offset {descriptor.Offset}. Expected {descriptor.Length} bytes, got {bytesRead}.");
        }

        chunkData = buffer;
        return true;
    }

    public ValueTask<ReadOnlyMemory<byte>?> GetChunkAsync(string hashHex, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_chunksByHash.TryGetValue(hashHex, out var descriptor))
        {
            return new ValueTask<ReadOnlyMemory<byte>?>((ReadOnlyMemory<byte>?)null);
        }

        byte[] buffer = new byte[descriptor.Length];
        int bytesRead = RandomAccess.Read(_fileHandle, buffer, descriptor.Offset);
        if (bytesRead != descriptor.Length)
        {
            throw new IOException($"Could not read full chunk from file at offset {descriptor.Offset}. Expected {descriptor.Length} bytes, got {bytesRead}.");
        }

        return new ValueTask<ReadOnlyMemory<byte>?>((ReadOnlyMemory<byte>?)buffer);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _fileHandle.Dispose();
        }
    }
}

/// <summary>
/// Delegate-backed implementation of IRemoteChunkSource for testing and flexible pipelining.
/// </summary>
public sealed class DelegateRemoteChunkSource : IRemoteChunkSource
{
    private readonly Func<ChunkDescriptor, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> _fetcher;

    public DelegateRemoteChunkSource(Func<ChunkDescriptor, CancellationToken, ValueTask<ReadOnlyMemory<byte>>> fetcher)
    {
        ArgumentNullException.ThrowIfNull(fetcher);
        _fetcher = fetcher;
    }

    public ValueTask<ReadOnlyMemory<byte>> FetchChunkAsync(ChunkDescriptor chunk, CancellationToken cancellationToken = default)
    {
        return _fetcher(chunk, cancellationToken);
    }
}

/// <summary>
/// In-memory dictionary-backed implementation of IRemoteChunkSource for testing.
/// </summary>
public sealed class MemoryRemoteChunkSource : IRemoteChunkSource
{
    private readonly Dictionary<string, byte[]> _chunks;

    public MemoryRemoteChunkSource(IDictionary<string, byte[]>? chunks = null)
    {
        _chunks = chunks != null
            ? new Dictionary<string, byte[]>(chunks, StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, byte[]>(StringComparer.OrdinalIgnoreCase);
    }

    public void AddChunk(string hashHex, byte[] data)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(hashHex);
        ArgumentNullException.ThrowIfNull(data);
        _chunks[hashHex.ToLowerInvariant()] = data;
    }

    public ValueTask<ReadOnlyMemory<byte>> FetchChunkAsync(ChunkDescriptor chunk, CancellationToken cancellationToken = default)
    {
        if (_chunks.TryGetValue(chunk.HashHex, out var bytes))
        {
            return new ValueTask<ReadOnlyMemory<byte>>(bytes);
        }

        throw new KeyNotFoundException($"Missing chunk with hash {chunk.HashHex} (index {chunk.Index}) was not available in remote source.");
    }
}
