namespace DeltaSync.Core.Causality;

/// <summary>
/// Immutable record capturing the result of a conflict resolution evaluation.
/// Specifies resulting vector clocks and filesystem targets for both primary and sibling files.
/// </summary>
/// <param name="Type">The categorical resolution action.</param>
/// <param name="CausalRelation">The strict partial order relationship evaluated between local and remote clocks.</param>
/// <param name="PrimaryPath">Relative path of the primary/original file.</param>
/// <param name="PrimaryVector">The resulting updated vector clock for the primary file.</param>
/// <param name="SiblingPath">Generated side-by-side path (e.g. "doc (NodeB conflicted).txt") if preserving side-by-side; null otherwise.</param>
/// <param name="SiblingVector">Vector clock assigned to the sibling file ($V_B$) if preserving side-by-side; null otherwise.</param>
/// <param name="LocalHash">The local file content hash at the time of resolution.</param>
/// <param name="RemoteHash">The remote file content hash at the time of resolution.</param>
public sealed record ConflictResolutionResult(
    ConflictResolutionType Type,
    CausalRelation CausalRelation,
    string PrimaryPath,
    VectorClock PrimaryVector,
    string? SiblingPath = null,
    VectorClock? SiblingVector = null,
    string? LocalHash = null,
    string? RemoteHash = null
);
