using System.Security.Cryptography;
using System.Text.RegularExpressions;
using DeltaSync.Core.Storage;

namespace DeltaSync.Cli;

public sealed record ConflictRecord(
    string RelativeConflictPath,
    string RelativeOriginalPath,
    string PeerId,
    bool OriginalExists,
    long ConflictSizeBytes,
    long OriginalSizeBytes,
    DateTimeOffset ConflictModifiedUtc,
    DateTimeOffset OriginalModifiedUtc,
    string? ConflictSha256 = null,
    string? OriginalSha256 = null,
    string? Reason = null);

/// <summary>
/// Scans a synchronization directory for ADR-0001 concurrent edit conflicts (side-by-side branch files).
/// Correlates conflicted copies with original files and reports divergence metrics.
/// </summary>
public static class ConflictScanner
{
    // Matches ADR-0001 format: "filename (NodeId conflicted).ext" or "filename (NodeId conflicted 2).ext"
    private static readonly Regex Adr0001Pattern = new(
        @"^(?<base>.+?)\s+\((?<peer>.+?)\s+conflicted(?:\s+(?<attempt>\d+))?\)(?<ext>\.[^.]*)?$",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    // Matches harness format: "filename.sync-conflict-peer.ext" or "filename.sync-conflict-ts-peer.ext"
    private static readonly Regex AltConflictPattern = new(
        @"^(?<base>.+?)\.sync-conflict-(?:(?<ts>\d+)-)?(?<peer>[^.]+)(?<ext>\.[^.]*)?$",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static readonly HashSet<string> IgnoredDirectories = new(StringComparer.OrdinalIgnoreCase)
    {
        ".deltasync",
        ".git",
        ".vs",
        ".idea",
        "bin",
        "obj"
    };

    public static async Task<IReadOnlyList<ConflictRecord>> ScanConflictsAsync(
        string syncPath,
        ISqliteStateStore? stateStore = null,
        CancellationToken ct = default)
    {
        var results = new List<ConflictRecord>();
        if (!Directory.Exists(syncPath))
        {
            return results;
        }

        var dirInfo = new DirectoryInfo(syncPath);
        var files = EnumerateFilesSafe(dirInfo);

        foreach (var file in files)
        {
            ct.ThrowIfCancellationRequested();

            string relativePath = Path.GetRelativePath(syncPath, file.FullName).Replace('\\', '/');
            string fileName = Path.GetFileName(relativePath);

            if (TryExtractConflictInfo(fileName, out string? originalFileName, out string? peerId))
            {
                string dir = Path.GetDirectoryName(relativePath)?.Replace('\\', '/') ?? string.Empty;
                string relativeOriginalPath = string.IsNullOrEmpty(dir)
                    ? originalFileName!
                    : $"{dir}/{originalFileName}";

                string fullOriginalPath = Path.Combine(syncPath, relativeOriginalPath);
                bool originalExists = File.Exists(fullOriginalPath);

                long originalSize = 0;
                DateTimeOffset originalModified = DateTimeOffset.MinValue;
                string? originalHash = null;

                if (originalExists)
                {
                    try
                    {
                        var origInfo = new FileInfo(fullOriginalPath);
                        originalSize = origInfo.Length;
                        originalModified = origInfo.LastWriteTimeUtc;
                        originalHash = await ComputeHashSafeAsync(fullOriginalPath, ct);
                    }
                    catch
                    {
                        // File may be locked or transient
                    }
                }

                long conflictSize = file.Length;
                DateTimeOffset conflictModified = file.LastWriteTimeUtc;
                string? conflictHash = await ComputeHashSafeAsync(file.FullName, ct);

                string reason = originalExists
                    ? (string.Equals(originalHash, conflictHash, StringComparison.OrdinalIgnoreCase)
                        ? "Identical content branch"
                        : "Concurrent offline edit fork (ADR-0001)")
                    : "Original file missing or deleted";

                results.Add(new ConflictRecord(
                    RelativeConflictPath: relativePath,
                    RelativeOriginalPath: relativeOriginalPath,
                    PeerId: peerId ?? "unknown",
                    OriginalExists: originalExists,
                    ConflictSizeBytes: conflictSize,
                    OriginalSizeBytes: originalSize,
                    ConflictModifiedUtc: conflictModified,
                    OriginalModifiedUtc: originalModified,
                    ConflictSha256: conflictHash,
                    OriginalSha256: originalHash,
                    Reason: reason));
            }
        }

        return results;
    }

    public static bool TryExtractConflictInfo(
        string fileName,
        out string? originalFileName,
        out string? peerId)
    {
        var match = Adr0001Pattern.Match(fileName);
        if (match.Success)
        {
            string baseName = match.Groups["base"].Value;
            string ext = match.Groups["ext"].Value;
            peerId = match.Groups["peer"].Value;
            originalFileName = $"{baseName}{ext}";
            return true;
        }

        var altMatch = AltConflictPattern.Match(fileName);
        if (altMatch.Success)
        {
            string baseName = altMatch.Groups["base"].Value;
            string ext = altMatch.Groups["ext"].Value;
            peerId = altMatch.Groups["peer"].Value;
            originalFileName = $"{baseName}{ext}";
            return true;
        }

        originalFileName = null;
        peerId = null;
        return false;
    }

    private static IEnumerable<FileInfo> EnumerateFilesSafe(DirectoryInfo root)
    {
        var stack = new Stack<DirectoryInfo>();
        stack.Push(root);

        while (stack.Count > 0)
        {
            var current = stack.Pop();
            FileInfo[] files;
            DirectoryInfo[] subDirs;

            try
            {
                files = current.GetFiles();
                subDirs = current.GetDirectories();
            }
            catch (UnauthorizedAccessException)
            {
                continue;
            }
            catch (DirectoryNotFoundException)
            {
                continue;
            }

            foreach (var file in files)
            {
                yield return file;
            }

            foreach (var sub in subDirs)
            {
                if (!IgnoredDirectories.Contains(sub.Name))
                {
                    stack.Push(sub);
                }
            }
        }
    }

    private static async Task<string?> ComputeHashSafeAsync(string fullPath, CancellationToken ct)
    {
        try
        {
            await using var stream = new FileStream(fullPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            byte[] hash = await SHA256.HashDataAsync(stream, ct);
            return Convert.ToHexString(hash).ToLowerInvariant();
        }
        catch
        {
            return null;
        }
    }
}
