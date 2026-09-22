namespace DeltaSync.Core.Causality;

/// <summary>
/// Helper for generating side-by-side conflicted file paths according to ADR-0001
/// and Vector Clock Engine Specification §7 and §11.
/// Format: "filename (MachineName conflicted).ext", incrementing to "filename (MachineName conflicted 2).ext"
/// on subsequent collisions up to a safe bound of 100 iterations.
/// </summary>
public static class ConflictedPathHelper
{
    public const int MaxCollisionAttempts = 100;

    // Use the Windows-invalid filename characters consistently on all platforms.
    private static readonly HashSet<char> InvalidCharacters = new(
        new[] { '<', '>', ':', '"', '/', '\\', '|', '?', '*' });

    /// <summary>
    /// Generates a conflict sibling path for the given file path and peer identifier.
    /// Checks for existing paths using the provided predicate to increment collision counters deterministically.
    /// </summary>
    /// <param name="relativePath">The relative path of the original file.</param>
    /// <param name="peerId">The peer node identifier associated with the conflicting update.</param>
    /// <param name="pathExists">Optional predicate to check whether a candidate path already exists on disk or in the index.</param>
    /// <returns>A collision-free sibling file path.</returns>
    /// <exception cref="ArgumentException">Thrown when relativePath or peerId is null, empty, or whitespace.</exception>
    /// <exception cref="InvalidOperationException">Thrown when 100 collision attempts are exceeded.</exception>
    public static string GenerateConflictedPath(
        string relativePath,
        string peerId,
        Func<string, bool>? pathExists = null)
    {
        if (string.IsNullOrWhiteSpace(relativePath))
        {
            throw new ArgumentException("Relative path cannot be null, empty, or whitespace.", nameof(relativePath));
        }

        if (string.IsNullOrWhiteSpace(peerId))
        {
            throw new ArgumentException("Peer ID cannot be null, empty, or whitespace.", nameof(peerId));
        }

        string sanitizedPeerId = SanitizePeerId(peerId.Trim());
        string normalizedPath = relativePath.Replace('\\', '/');

        string directory = Path.GetDirectoryName(normalizedPath)?.Replace('\\', '/') ?? string.Empty;
        string fileName = Path.GetFileName(normalizedPath);

        string baseName;
        string extension;

        // Handle dotfiles like .gitignore where GetFileNameWithoutExtension is empty
        if (fileName.StartsWith('.') && fileName.IndexOf('.', 1) < 0)
        {
            baseName = fileName;
            extension = string.Empty;
        }
        else
        {
            baseName = Path.GetFileNameWithoutExtension(fileName);
            extension = Path.GetExtension(fileName);
        }

        for (int attempt = 1; attempt <= MaxCollisionAttempts; attempt++)
        {
            string suffix = attempt == 1
                ? $" ({sanitizedPeerId} conflicted)"
                : $" ({sanitizedPeerId} conflicted {attempt})";

            string candidateFileName = $"{baseName}{suffix}{extension}";
            string candidatePath = string.IsNullOrEmpty(directory)
                ? candidateFileName
                : $"{directory}/{candidateFileName}";

            if (pathExists == null || !pathExists(candidatePath))
            {
                return candidatePath;
            }
        }

        throw new InvalidOperationException(
            $"Exceeded maximum collision resolution attempts ({MaxCollisionAttempts}) for conflicted file '{relativePath}'.");
    }

    /// <summary>
    /// Sanitizes a peer ID to be safe for inclusion in a file system path name.
    /// Replaces invalid filename characters with underscores.
    /// </summary>
    public static string SanitizePeerId(string peerId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(peerId);

        // Use the Windows-invalid filename characters consistently on all platforms.
        var invalidCharacters = InvalidCharacters;

        return string.Concat(
            peerId.Select(character =>
                invalidCharacters.Contains(character) ? '_' : character));
    }
}
