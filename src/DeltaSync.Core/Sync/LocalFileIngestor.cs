using System.Security.Cryptography;
using DeltaSync.Core.Causality;
using DeltaSync.Core.Chunking;
using DeltaSync.Core.Models;
using DeltaSync.Core.Storage;

namespace DeltaSync.Core.Sync;

/// <summary>
/// Pipeline engine that streams local files through FastCDC, fingerprints content chunks,
/// advances causal Vector Clocks, and atomically persists file metadata and Merkle tree state in SQLite.
/// </summary>
public sealed class LocalFileIngestor
{
    private readonly string _rootDirectory;
    private readonly string _canonicalRoot;
    private readonly ISqliteStateStore _stateStore;
    private readonly string _localPeerId;
    private readonly FastCdcConfig _cdcConfig;

    /// <summary>
    /// Directories and path prefixes ignored by the local ingestion engine.
    /// </summary>
    public static readonly IReadOnlyList<string> DefaultIgnoredPrefixes = new[]
    {
        ".deltasync",
        ".git"
    };

    public LocalFileIngestor(
        string rootDirectory,
        ISqliteStateStore stateStore,
        string localPeerId,
        FastCdcConfig? cdcConfig = null)
    {
        if (string.IsNullOrWhiteSpace(rootDirectory))
            throw new ArgumentException("Root directory cannot be null or whitespace.", nameof(rootDirectory));
        _rootDirectory = Path.GetFullPath(rootDirectory);
        _canonicalRoot = _rootDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + Path.DirectorySeparatorChar;

        _stateStore = stateStore ?? throw new ArgumentNullException(nameof(stateStore));
        _localPeerId = !string.IsNullOrWhiteSpace(localPeerId)
            ? localPeerId.Trim()
            : throw new ArgumentException("Local peer ID cannot be null or whitespace.", nameof(localPeerId));
        _cdcConfig = cdcConfig ?? FastCdcConfig.Default;
    }

    public string RootDirectory => _rootDirectory;
    public string LocalPeerId => _localPeerId;
    public FastCdcConfig CdcConfig => _cdcConfig;
    public ISqliteStateStore StateStore => _stateStore;

    /// <summary>
    /// Checks whether a given relative path represents an excluded system/metadata path.
    /// </summary>
    public static bool IsExcludedPath(string relativePath)
    {
        if (string.IsNullOrWhiteSpace(relativePath)) return true;
        string normalized = FileManifest.NormalizePath(relativePath);

        foreach (var prefix in DefaultIgnoredPrefixes)
        {
            if (normalized.Equals(prefix, StringComparison.OrdinalIgnoreCase) ||
                normalized.StartsWith(prefix + "/", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        if (normalized.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase) ||
            normalized.EndsWith(".crswap", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return false;
    }

    /// <summary>
    /// Processes and ingests a single local file into the state store with FastCDC chunking and vector clock advancement.
    /// Returns null if the file was deleted, missing, or excluded.
    /// </summary>
    public async Task<FileMetadata?> IngestFileAsync(string relativePath, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrWhiteSpace(relativePath))
            throw new ArgumentException("Relative path cannot be null or whitespace.", nameof(relativePath));

        string normalized = FileManifest.NormalizePath(relativePath);
        if (IsExcludedPath(normalized))
        {
            return null;
        }

        string fullPath = Path.GetFullPath(Path.Combine(_rootDirectory, normalized));
        if (!fullPath.StartsWith(_canonicalRoot, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Security boundary violation: '{relativePath}' resolves outside root directory.");
        }

        if (!File.Exists(fullPath))
        {
            return null;
        }

        // Handle file sharing lock contention with exponential backoff up to 4 attempts
        const int maxAttempts = 4;
        FileStream? stream = null;
        for (int attempt = 0; attempt < maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                stream = new FileStream(
                    fullPath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.ReadWrite | FileShare.Delete,
                    bufferSize: 64 * 1024,
                    useAsync: true);
                break;
            }
            catch (IOException ex) when (attempt < maxAttempts - 1 && IsSharingViolation(ex))
            {
                int delayMs = (int)(50 * Math.Pow(2, attempt)); // 50ms, 100ms, 200ms
                await Task.Delay(delayMs, cancellationToken).ConfigureAwait(false);
            }
        }

        if (stream == null)
        {
            // If still locked or unavailable, attempt one final open which will bubble exception if truly inaccessible
            stream = new FileStream(
                fullPath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete,
                bufferSize: 64 * 1024,
                useAsync: true);
        }

        FileManifest manifest;
        await using (stream.ConfigureAwait(false))
        {
            manifest = await StreamingFastCdcReader.ReadManifestAsync(
                normalized,
                stream,
                _cdcConfig,
                cancellationToken: cancellationToken).ConfigureAwait(false);
        }

        var fileInfo = new FileInfo(fullPath);
        DateTimeOffset modifiedUtc = fileInfo.Exists ? fileInfo.LastWriteTimeUtc : DateTimeOffset.UtcNow;

        var existing = await _stateStore.GetFileAsync(normalized, cancellationToken).ConfigureAwait(false);

        VectorClock clock;
        int version;

        if (existing != null)
        {
            // If file exists and content hash has not changed, do not advance clock or write redundant DB updates (F4 idempotency)
            if (!existing.IsDeleted && string.Equals(existing.RootHash, manifest.RootHash, StringComparison.OrdinalIgnoreCase))
            {
                return existing;
            }

            // Monotonic causal progress on local modification
            clock = existing.Clock.Tick(_localPeerId);
            version = existing.Version + 1;
        }
        else
        {
            clock = VectorClock.Empty.Tick(_localPeerId);
            version = 1;
        }

        var metadata = new FileMetadata(
            relativePath: normalized,
            sizeBytes: manifest.FileSize,
            rootHash: manifest.RootHash,
            modifiedUtc: modifiedUtc,
            clock: clock,
            isDeleted: false,
            version: version);

        await _stateStore.UpsertFileAsync(metadata, manifest.Chunks, cancellationToken).ConfigureAwait(false);
        return metadata;
    }

    /// <summary>
    /// Atomically marks a file deleted (tombstone) in the state store and updates bottom-up Merkle prefix digests.
    /// </summary>
    public async Task<bool> DeleteFileAsync(string relativePath, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrWhiteSpace(relativePath))
            throw new ArgumentException("Relative path cannot be null or whitespace.", nameof(relativePath));

        string normalized = FileManifest.NormalizePath(relativePath);
        if (IsExcludedPath(normalized))
        {
            return false;
        }

        bool deleted = await _stateStore.DeleteFileAsync(normalized, cancellationToken).ConfigureAwait(false);
        if (deleted)
        {
            return true;
        }

        // Cascade recursive directory deletions: check if any active records exist with prefix normalized + "/"
        string dirPrefix = normalized + "/";
        var activeFiles = await _stateStore.GetAllFilesAsync(includeDeleted: false, cancellationToken).ConfigureAwait(false);
        bool anyChildDeleted = false;

        foreach (var file in activeFiles)
        {
            if (file.RelativePath.StartsWith(dirPrefix, StringComparison.OrdinalIgnoreCase))
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (await _stateStore.DeleteFileAsync(file.RelativePath, cancellationToken).ConfigureAwait(false))
                {
                    anyChildDeleted = true;
                }
            }
        }

        return anyChildDeleted;
    }

    /// <summary>
    /// Recursively scans the entire root directory and ingests all untracked or modified files.
    /// Useful for initial synchronization and full directory re-indexing.
    /// </summary>
    public async Task<int> IngestAllAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!Directory.Exists(_rootDirectory))
        {
            return 0;
        }

        int count = 0;
        var dirInfo = new DirectoryInfo(_rootDirectory);
        foreach (var file in dirInfo.EnumerateFiles("*", SearchOption.AllDirectories))
        {
            cancellationToken.ThrowIfCancellationRequested();

            string relativePath = Path.GetRelativePath(_rootDirectory, file.FullName);
            string normalized = FileManifest.NormalizePath(relativePath);

            if (IsExcludedPath(normalized))
            {
                continue;
            }

            var ingested = await IngestFileAsync(normalized, cancellationToken).ConfigureAwait(false);
            if (ingested != null)
            {
                count++;
            }
        }

        return count;
    }

    private static bool IsSharingViolation(IOException ex)
    {
        int hr = ex.HResult;
        return hr == unchecked((int)0x80070020) || // ERROR_SHARING_VIOLATION
               hr == unchecked((int)0x80070021) || // ERROR_LOCK_VIOLATION
               hr == 32 ||
               hr == 33;
    }
}
