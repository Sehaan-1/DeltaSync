namespace DeltaSync.Core.Causality;

/// <summary>
/// State record representing the current local file state known to this peer.
/// </summary>
/// <param name="RelativePath">Relative path within the synced folder.</param>
/// <param name="ContentHash">Content hash (e.g. SHA-256) of the local file, or null if deleted/untracked.</param>
/// <param name="Clock">The local vector clock associated with this file.</param>
public sealed record LocalFileState(
    string RelativePath,
    string? ContentHash,
    VectorClock Clock
);

/// <summary>
/// State record representing an incoming remote file update from a peer.
/// </summary>
/// <param name="RelativePath">Relative path within the synced folder.</param>
/// <param name="PeerId">Identifier of the remote peer originating or transmitting the update.</param>
/// <param name="ContentHash">Content hash (e.g. SHA-256) of the remote payload.</param>
/// <param name="Clock">The remote vector clock associated with this update.</param>
public sealed record RemoteFileUpdate(
    string RelativePath,
    string PeerId,
    string? ContentHash,
    VectorClock Clock
);

/// <summary>
/// Core conflict detection and branch resolution engine.
/// Evaluates causality between local and remote file states according to ADR-0001
/// and Vector Clock Engine Specification §3 Step 4.
/// Guarantees Invariant I2 (zero silent overwrite) and mathematical convergence without sync storms.
/// </summary>
public static class ConflictResolver
{
    /// <summary>
    /// Resolves causality for a file in a local directory, automatically checking for candidate file collisions on disk.
    /// </summary>
    public static ConflictResolutionResult ResolveForDirectory(
        string baseDirectory,
        string localPeerId,
        string relativePath,
        string? localHash,
        VectorClock? localClock,
        string remotePeerId,
        string? remoteHash,
        VectorClock remoteClock)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(baseDirectory);

        return Resolve(
            localPeerId: localPeerId,
            relativePath: relativePath,
            localHash: localHash,
            localClock: localClock,
            remotePeerId: remotePeerId,
            remoteHash: remoteHash,
            remoteClock: remoteClock,
            pathExists: p => File.Exists(Path.Combine(baseDirectory, p)));
    }

    /// <summary>
    /// Applies the resolution action to a local directory, writing remote content to the primary or sibling path.
    /// Enforces Invariant I2 (zero silent overwrite) by preserving both copies on disk.
    /// </summary>
    /// <param name="resolution">The resolution result to apply.</param>
    /// <param name="baseDirectory">The root directory of the synced folder.</param>
    /// <param name="remoteContent">The byte payload of the remote file, required if applying remote or preserving side-by-side.</param>
    public static void ApplyToDirectory(
        ConflictResolutionResult resolution,
        string baseDirectory,
        byte[]? remoteContent = null)
    {
        ArgumentNullException.ThrowIfNull(resolution);
        ArgumentException.ThrowIfNullOrWhiteSpace(baseDirectory);

        switch (resolution.Type)
        {
            case ConflictResolutionType.ApplyRemote:
                if (remoteContent != null)
                {
                    string fullPath = Path.Combine(baseDirectory, resolution.PrimaryPath);
                    string? dir = Path.GetDirectoryName(fullPath);
                    if (!string.IsNullOrEmpty(dir))
                    {
                        Directory.CreateDirectory(dir);
                    }
                    File.WriteAllBytes(fullPath, remoteContent);
                }
                break;

            case ConflictResolutionType.PreserveSideBySide:
                if (!string.IsNullOrEmpty(resolution.SiblingPath) && remoteContent != null)
                {
                    string siblingFullPath = Path.Combine(baseDirectory, resolution.SiblingPath);
                    string? dir = Path.GetDirectoryName(siblingFullPath);
                    if (!string.IsNullOrEmpty(dir))
                    {
                        Directory.CreateDirectory(dir);
                    }
                    File.WriteAllBytes(siblingFullPath, remoteContent);
                }
                break;

            case ConflictResolutionType.NoOp:
            case ConflictResolutionType.RejectObsolete:
            case ConflictResolutionType.MergeIdentical:
            default:
                break;
        }
    }

    /// <summary>
    /// Resolves the causal relationship and required reconciliation action between local and remote states.
    /// </summary>
    /// <param name="localPeerId">The identifier of this local node.</param>
    /// <param name="localState">The current local state of the file, or null if the file does not exist locally.</param>
    /// <param name="remoteUpdate">The incoming remote file update.</param>
    /// <param name="pathExists">Optional predicate to check whether a candidate sibling path exists.</param>
    /// <returns>A <see cref="ConflictResolutionResult"/> describing the exact action and resulting vector clocks.</returns>
    public static ConflictResolutionResult Resolve(
        string localPeerId,
        LocalFileState? localState,
        RemoteFileUpdate remoteUpdate,
        Func<string, bool>? pathExists = null)
    {
        ArgumentNullException.ThrowIfNull(remoteUpdate);

        return Resolve(
            localPeerId: localPeerId,
            relativePath: remoteUpdate.RelativePath,
            localHash: localState?.ContentHash,
            localClock: localState?.Clock ?? VectorClock.Empty,
            remotePeerId: remoteUpdate.PeerId,
            remoteHash: remoteUpdate.ContentHash,
            remoteClock: remoteUpdate.Clock,
            pathExists: pathExists);
    }

    /// <summary>
    /// Evaluates causality and decides the reconciliation action for given file states.
    /// </summary>
    /// <param name="localPeerId">The identifier of this local node.</param>
    /// <param name="relativePath">The relative path of the file.</param>
    /// <param name="localHash">The local file content hash, or null if empty/absent.</param>
    /// <param name="localClock">The local vector clock.</param>
    /// <param name="remotePeerId">The identifier of the remote peer.</param>
    /// <param name="remoteHash">The remote file content hash.</param>
    /// <param name="remoteClock">The remote vector clock.</param>
    /// <param name="pathExists">Optional predicate to check whether a sibling path exists.</param>
    /// <returns>A <see cref="ConflictResolutionResult"/> describing the exact action and resulting vector clocks.</returns>
    public static ConflictResolutionResult Resolve(
        string localPeerId,
        string relativePath,
        string? localHash,
        VectorClock? localClock,
        string remotePeerId,
        string? remoteHash,
        VectorClock remoteClock,
        Func<string, bool>? pathExists = null)
    {
        if (string.IsNullOrWhiteSpace(localPeerId))
        {
            throw new ArgumentException("Local peer ID cannot be null, empty, or whitespace.", nameof(localPeerId));
        }

        if (string.IsNullOrWhiteSpace(relativePath))
        {
            throw new ArgumentException("Relative path cannot be null, empty, or whitespace.", nameof(relativePath));
        }

        if (string.IsNullOrWhiteSpace(remotePeerId))
        {
            throw new ArgumentException("Remote peer ID cannot be null, empty, or whitespace.", nameof(remotePeerId));
        }

        ArgumentNullException.ThrowIfNull(remoteClock);

        VectorClock vA = localClock ?? VectorClock.Empty;
        VectorClock vB = remoteClock;

        CausalRelation relation = vA.Compare(vB);

        switch (relation)
        {
            // Branch 1 (Equal): Content is identical or already aligned. Action: No-op.
            case CausalRelation.Equal:
                return new ConflictResolutionResult(
                    Type: ConflictResolutionType.NoOp,
                    CausalRelation: CausalRelation.Equal,
                    PrimaryPath: relativePath,
                    PrimaryVector: vA,
                    SiblingPath: null,
                    SiblingVector: null,
                    LocalHash: localHash,
                    RemoteHash: remoteHash);

            // Branch 2 (Before): Remote version strictly happened-after local version (VA < VB).
            // Action: Overwrite local file with remote payload, set local vector to VB.
            case CausalRelation.Before:
                return new ConflictResolutionResult(
                    Type: ConflictResolutionType.ApplyRemote,
                    CausalRelation: CausalRelation.Before,
                    PrimaryPath: relativePath,
                    PrimaryVector: vB,
                    SiblingPath: null,
                    SiblingVector: null,
                    LocalHash: localHash,
                    RemoteHash: remoteHash);

            // Branch 3 (After): Remote version is an obsolete echo (VB < VA).
            // Action: Reject update, retain local state.
            case CausalRelation.After:
                return new ConflictResolutionResult(
                    Type: ConflictResolutionType.RejectObsolete,
                    CausalRelation: CausalRelation.After,
                    PrimaryPath: relativePath,
                    PrimaryVector: vA,
                    SiblingPath: null,
                    SiblingVector: null,
                    LocalHash: localHash,
                    RemoteHash: remoteHash);

            // Branch 4 (Concurrent): Divergent concurrent modification (VA || VB).
            case CausalRelation.Concurrent:
            default:
                bool identicalContent = !string.IsNullOrEmpty(localHash) &&
                                        string.Equals(localHash, remoteHash, StringComparison.OrdinalIgnoreCase);

                if (identicalContent)
                {
                    // Branch 4a: Identical content; merge vectors component-wise without duplicating files.
                    VectorClock mergedClock = vA.Merge(vB);
                    return new ConflictResolutionResult(
                        Type: ConflictResolutionType.MergeIdentical,
                        CausalRelation: CausalRelation.Concurrent,
                        PrimaryPath: relativePath,
                        PrimaryVector: mergedClock,
                        SiblingPath: null,
                        SiblingVector: null,
                        LocalHash: localHash,
                        RemoteHash: remoteHash);
                }
                else
                {
                    // Branch 4b: True semantic conflict (HA != HB).
                    // 1. Retain local file at original path.
                    // 2. Generate side-by-side sibling path with machine attribution (ADR-0001).
                    // 3. Assign sibling file remote vector VB.
                    // 4. Unify main branch vector: V'A = Tick(VA ⊔ VB, localPeerId).
                    string siblingPath = ConflictedPathHelper.GenerateConflictedPath(
                        relativePath,
                        remotePeerId,
                        pathExists);

                    VectorClock supremum = vA.Merge(vB);
                    VectorClock unifiedClock = supremum.Tick(localPeerId);

                    return new ConflictResolutionResult(
                        Type: ConflictResolutionType.PreserveSideBySide,
                        CausalRelation: CausalRelation.Concurrent,
                        PrimaryPath: relativePath,
                        PrimaryVector: unifiedClock,
                        SiblingPath: siblingPath,
                        SiblingVector: vB,
                        LocalHash: localHash,
                        RemoteHash: remoteHash);
                }
        }
    }
}
