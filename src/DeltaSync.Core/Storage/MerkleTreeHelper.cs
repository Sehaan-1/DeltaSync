using System.Security.Cryptography;
using System.Text;
using DeltaSync.Core.Chunking;

namespace DeltaSync.Core.Storage;

/// <summary>
/// Deterministic calculation and prefix navigation for hierarchical directory Merkle prefix trees.
/// Follows Spec §3 Step 6:
/// H(D) = SHA256( sum_{f in children(D)} name(f) \circ R_f || sum_{S in subdirs(D)} name(S) \circ H(S) )
/// </summary>
public static class MerkleTreeHelper
{
    /// <summary>
    /// SHA-256 digest of an empty stream or empty directory.
    /// </summary>
    public const string EmptyNodeHash = FileManifest.EmptyRootHash;

    /// <summary>
    /// Normalizes a directory prefix: trims whitespace, normalizes backslashes to forward slashes,
    /// strips leading/trailing slashes, validates against path traversal, and returns lowercase.
    /// Root directory is represented as an empty string ("").
    /// </summary>
    public static string NormalizePrefix(string? prefix)
    {
        if (string.IsNullOrWhiteSpace(prefix))
        {
            return string.Empty;
        }

        string normalized = prefix.Trim().Replace('\\', '/').Trim('/');
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return string.Empty;
        }

        string[] segments = normalized.Split('/');
        foreach (string seg in segments)
        {
            if (seg is "." or "..")
            {
                throw new ArgumentException($"Prefix '{prefix}' contains invalid traversal component '{seg}'.", nameof(prefix));
            }
        }

        return normalized.ToLowerInvariant();
    }

    /// <summary>
    /// Resolves the parent directory prefix for a given prefix.
    /// Returns null if the prefix is root ("").
    /// </summary>
    public static string? GetParentPrefix(string prefix)
    {
        string norm = NormalizePrefix(prefix);
        if (norm.Length == 0)
        {
            return null;
        }

        int lastSlash = norm.LastIndexOf('/');
        return lastSlash == -1 ? string.Empty : norm[..lastSlash];
    }

    /// <summary>
    /// Computes the ascending prefix chain from the file's containing directory up to the root ("").
    /// Ordered from deepest directory to root.
    /// For example, "docs/arch/spec.md" produces ["docs/arch", "docs", ""].
    /// A root file "app.cs" produces [""].
    /// </summary>
    public static IReadOnlyList<string> GetPrefixChain(string relativePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(relativePath);

        string normalized = relativePath.Trim().Replace('\\', '/').TrimStart('/');
        int lastSlash = normalized.LastIndexOf('/');
        string dir = lastSlash == -1 ? string.Empty : normalized[..lastSlash];

        var chain = new List<string>();
        string current = NormalizePrefix(dir);
        while (true)
        {
            chain.Add(current);
            if (current.Length == 0)
            {
                break;
            }
            current = GetParentPrefix(current) ?? string.Empty;
        }

        return chain;
    }

    /// <summary>
    /// Computes the deterministic SHA-256 Merkle node digest for a directory prefix given its
    /// direct child files and immediate child subdirectories.
    /// </summary>
    public static string ComputeNodeHash(
        IEnumerable<(string Name, string RootHash)> files,
        IEnumerable<(string Name, string NodeHash)> subdirs)
    {
        ArgumentNullException.ThrowIfNull(files);
        ArgumentNullException.ThrowIfNull(subdirs);

        var fileList = files
            .Select(f => (Name: f.Name.ToLowerInvariant(), RootHash: f.RootHash.ToLowerInvariant()))
            .OrderBy(f => f.Name, StringComparer.Ordinal)
            .ToList();

        var subdirList = subdirs
            .Select(s => (Name: s.Name.ToLowerInvariant(), NodeHash: s.NodeHash.ToLowerInvariant()))
            .OrderBy(s => s.Name, StringComparer.Ordinal)
            .ToList();

        if (fileList.Count == 0 && subdirList.Count == 0)
        {
            return EmptyNodeHash;
        }

        using var ms = new MemoryStream();
        using (var writer = new StreamWriter(ms, new UTF8Encoding(false), leaveOpen: true))
        {
            foreach (var (name, rootHash) in fileList)
            {
                writer.Write("file\0");
                writer.Write(name);
                writer.Write('\0');
                writer.Write(rootHash);
                writer.Write('\n');
            }

            foreach (var (name, subHash) in subdirList)
            {
                writer.Write("dir\0");
                writer.Write(name);
                writer.Write('\0');
                writer.Write(subHash);
                writer.Write('\n');
            }

            writer.Flush();
        }

        ms.Position = 0;
        byte[] hashBytes = SHA256.HashData(ms.ToArray());
        return Convert.ToHexString(hashBytes).ToLowerInvariant();
    }
}
