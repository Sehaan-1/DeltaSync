namespace DeltaSync.Core.Causality;

/// <summary>
/// Represents the four possible causal relationships between two vector clocks
/// under the partial order defined by Parker et al. (1983) and Mattern (1989).
/// </summary>
public enum CausalRelation
{
    /// <summary>
    /// V1 and V2 represent identical causal states: ∀p, V1[p] == V2[p].
    /// </summary>
    Equal,

    /// <summary>
    /// V1 strictly happened before V2: (∀p, V1[p] &lt;= V2[p]) ∧ (∃p, V1[p] &lt; V2[p]).
    /// V2 strictly dominates and supersedes V1.
    /// </summary>
    Before,

    /// <summary>
    /// V1 strictly happened after V2: (∀p, V1[p] &gt;= V2[p]) ∧ (∃p, V1[p] &gt; V2[p]).
    /// V1 strictly dominates and supersedes V2.
    /// </summary>
    After,

    /// <summary>
    /// V1 and V2 are causally divergent / concurrent:
    /// ∃p, q: V1[p] &gt; V2[p] ∧ V1[q] &lt; V2[q].
    /// Represents an uncoordinated concurrent edit requiring conflict branch resolution.
    /// </summary>
    Concurrent
}
