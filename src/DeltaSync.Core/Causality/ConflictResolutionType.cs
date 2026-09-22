namespace DeltaSync.Core.Causality;

/// <summary>
/// Defines the resolution actions resulting from causal comparison of local and remote file states
/// according to ADR-0001 and Vector Clock Engine Specification §3 Step 4.
/// </summary>
public enum ConflictResolutionType
{
    /// <summary>
    /// Vectors are identical (VA == VB); content is aligned. No filesystem or state change needed.
    /// </summary>
    NoOp = 1,

    /// <summary>
    /// Remote vector strictly dominates local (VA &lt; VB). Remote content supersedes local.
    /// </summary>
    ApplyRemote = 2,

    /// <summary>
    /// Local vector strictly dominates remote (VB &lt; VA). Remote update is an obsolete echo.
    /// </summary>
    RejectObsolete = 3,

    /// <summary>
    /// Vectors are concurrent (VA || VB) but content hashes are identical (HA == HB).
    /// Vectors merge component-wise without generating duplicate files.
    /// </summary>
    MergeIdentical = 4,

    /// <summary>
    /// Vectors are concurrent (VA || VB) and content hashes differ (HA != HB).
    /// Both copies are preserved side-by-side (ADR-0001) with machine attribution.
    /// </summary>
    PreserveSideBySide = 5
}
