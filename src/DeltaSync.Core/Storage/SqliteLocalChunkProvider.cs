using System.Security.Cryptography;
using DeltaSync.Core.Chunking;

namespace DeltaSync.Core.Storage;

/// <summary>
/// State-store-backed zero-copy chunk provider that resolves physical file byte ranges via inverted index
/// and reads chunk slices directly from local files using lock-free RandomAccess.
/// </summary>
public sealed class SqliteLocalChunkProvider : ILocalChunkProvider, IDisposable
{
    private readonly ISqliteStateStore _stateStore;
    private readonly string _rootDirectory;
    private bool _disposed;

    public SqliteLocalChunkProvider(ISqliteStateStore stateStore, string rootDirectory)
    {
        _stateStore = stateStore ?? throw new ArgumentNullException(nameof(stateStore));
        _rootDirectory = rootDirectory ?? throw new ArgumentNullException(nameof(rootDirectory));

        if (!Directory.Exists(_rootDirectory))
        {
            throw new DirectoryNotFoundException($"Root sync directory not found at: {_rootDirectory}");
        }
    }

    public bool TryGetChunk(string hashHex, out ReadOnlyMemory<byte> chunkData)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        try
        {
            var task = GetChunkAsync(hashHex).AsTask();
            var result = task.GetAwaiter().GetResult();
            if (result.HasValue)
            {
                chunkData = result.Value;
                return true;
            }
        }
        catch
        {
            // Fall through to false
        }

        chunkData = default;
        return false;
    }

    public async ValueTask<ReadOnlyMemory<byte>?> GetChunkAsync(string hashHex, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrWhiteSpace(hashHex);

        var locations = await _stateStore.GetChunkLocationsAsync(hashHex, cancellationToken).ConfigureAwait(false);
        if (locations.Count == 0)
        {
            return null;
        }

        ChunkLocation? location = null;
        string? resolvedFullPath = null;

        foreach (var loc in locations)
        {
            string candidatePath = Path.Combine(_rootDirectory, loc.RelativePath.Replace('/', Path.DirectorySeparatorChar));
            if (File.Exists(candidatePath))
            {
                location = loc;
                resolvedFullPath = candidatePath;
                break;
            }
        }

        if (location == null || resolvedFullPath == null)
        {
            return null;
        }

        byte[] buffer = new byte[location.Length];
        using (var fileHandle = File.OpenHandle(resolvedFullPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            int bytesRead = RandomAccess.Read(fileHandle, buffer, location.Offset);
            if (bytesRead != location.Length)
            {
                throw new IOException($"Could not read full chunk from '{location.RelativePath}' at offset {location.Offset}. Expected {location.Length} bytes, got {bytesRead}.");
            }
        }

        // Verify SHA-256 integrity
        byte[] computedHash = SHA256.HashData(buffer);
        string computedHashHex = Convert.ToHexString(computedHash).ToLowerInvariant();
        if (!computedHashHex.Equals(hashHex.Trim(), StringComparison.OrdinalIgnoreCase))
        {
            throw new ChunkIntegrityException(
                $"Chunk integrity verification failed for '{location.RelativePath}' at offset {location.Offset}. Expected {hashHex}, computed {computedHashHex}.");
        }

        return new ReadOnlyMemory<byte>(buffer);
    }

    public void Dispose()
    {
        _disposed = true;
    }
}
