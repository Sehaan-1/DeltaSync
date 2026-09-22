using System.Buffers.Binary;
using System.Collections;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DeltaSync.Core.Causality;

/// <summary>
/// Immutable value object representing a version vector over peer node identifiers.
/// Stored internally as a compact, canonical sorted array of (PeerId, Counter) pairs.
/// Implements partial order comparison, supremum merging, and tick operations
/// according to Parker et al. (1983) and Mattern (1989).
/// </summary>
[JsonConverter(typeof(VectorClockJsonConverter))]
public sealed class VectorClock : IReadOnlyDictionary<string, ulong>, IEquatable<VectorClock>
{
    private static readonly StringComparer PeerComparer = StringComparer.OrdinalIgnoreCase;

    private readonly KeyValuePair<string, ulong>[] _entries;

    /// <summary>
    /// Singleton empty vector clock representing the causal origin (zero counts for all peers).
    /// </summary>
    public static readonly VectorClock Empty = new(Array.Empty<KeyValuePair<string, ulong>>());

    private VectorClock(KeyValuePair<string, ulong>[] entries)
    {
        _entries = entries;
    }

    /// <summary>
    /// Gets the number of non-zero peer entries tracked in this vector clock.
    /// </summary>
    public int Count => _entries.Length;

    /// <summary>
    /// Gets a value indicating whether this vector clock has no active entries (origin state).
    /// </summary>
    public bool IsEmpty => _entries.Length == 0;

    /// <summary>
    /// Gets the collection of peer identifiers in this vector clock, in canonical sorted order.
    /// </summary>
    public IEnumerable<string> Keys => _entries.Select(e => e.Key);

    /// <summary>
    /// Gets the collection of non-zero counter values in this vector clock, in canonical sorted order.
    /// </summary>
    public IEnumerable<ulong> Values => _entries.Select(e => e.Value);

    /// <summary>
    /// Gets the counter value for the specified peer identifier.
    /// If the peer has not recorded an event, returns 0 by definition.
    /// </summary>
    /// <param name="peerId">The peer node identifier.</param>
    /// <returns>The monotonic counter value, or 0 if absent.</returns>
    public ulong this[string peerId]
    {
        get
        {
            if (string.IsNullOrWhiteSpace(peerId))
            {
                throw new ArgumentException("Peer ID cannot be null, empty, or whitespace.", nameof(peerId));
            }

            int idx = BinarySearch(peerId.Trim());
            return idx >= 0 ? _entries[idx].Value : 0UL;
        }
    }

    /// <summary>
    /// Creates a vector clock from key-value pairs of peer identifiers and counters.
    /// Entries with counter value 0 are omitted to preserve sparse representation.
    /// </summary>
    public static VectorClock Create(IEnumerable<KeyValuePair<string, ulong>> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        var dict = new Dictionary<string, ulong>(PeerComparer);

        foreach (var kvp in entries)
        {
            string key = ValidateAndNormalizePeerId(kvp.Key);
            if (kvp.Value > 0)
            {
                dict[key] = kvp.Value;
            }
            else
            {
                dict.Remove(key);
            }
        }

        if (dict.Count == 0)
        {
            return Empty;
        }

        var array = dict.OrderBy(k => k.Key, PeerComparer).ToArray();
        return new VectorClock(array);
    }

    /// <summary>
    /// Creates a vector clock from inline peer identifier and counter tuples.
    /// </summary>
    public static VectorClock Create(params (string PeerId, ulong Counter)[] entries)
    {
        ArgumentNullException.ThrowIfNull(entries);

        var dict = new Dictionary<string, ulong>(PeerComparer);

        foreach (var (peerId, counter) in entries)
        {
            string key = ValidateAndNormalizePeerId(peerId);
            if (counter > 0)
            {
                dict[key] = counter;
            }
            else
            {
                dict.Remove(key);
            }
        }

        if (dict.Count == 0)
        {
            return Empty;
        }

        var array = dict.OrderBy(k => k.Key, PeerComparer).ToArray();
        return new VectorClock(array);
    }

    /// <summary>
    /// Returns a new vector clock with the counter for the specified local peer incremented by 1.
    /// Satisfies Invariant I1: Monotonic local progress.
    /// </summary>
    /// <param name="peerId">The local peer node identifier.</param>
    /// <returns>A new <see cref="VectorClock"/> instance.</returns>
    public VectorClock Tick(string peerId)
    {
        string key = ValidateAndNormalizePeerId(peerId);
        int idx = BinarySearch(key);

        KeyValuePair<string, ulong>[] result;
        if (idx >= 0)
        {
            result = (KeyValuePair<string, ulong>[])_entries.Clone();
            result[idx] = new KeyValuePair<string, ulong>(_entries[idx].Key, checked(_entries[idx].Value + 1UL));
        }
        else
        {
            int insertIdx = ~idx;
            result = new KeyValuePair<string, ulong>[_entries.Length + 1];
            Array.Copy(_entries, 0, result, 0, insertIdx);
            result[insertIdx] = new KeyValuePair<string, ulong>(key, 1UL);
            Array.Copy(_entries, insertIdx, result, insertIdx + 1, _entries.Length - insertIdx);
        }

        return new VectorClock(result);
    }

    /// <summary>
    /// Computes the component-wise supremum (least upper bound) of two vector clocks:
    /// V_sup[p] = max(V_this[p], V_other[p]).
    /// Uses an efficient two-pointer linear merge over sorted entries.
    /// </summary>
    /// <param name="other">The other vector clock to merge with.</param>
    /// <returns>A new vector clock strictly dominating or equal to both inputs.</returns>
    public VectorClock Merge(VectorClock other)
    {
        ArgumentNullException.ThrowIfNull(other);

        if (ReferenceEquals(this, other) || other.IsEmpty)
        {
            return this;
        }

        if (IsEmpty)
        {
            return other;
        }

        var a = _entries;
        var b = other._entries;

        var result = new KeyValuePair<string, ulong>[a.Length + b.Length];
        int i = 0, j = 0, k = 0;

        while (i < a.Length && j < b.Length)
        {
            int cmp = PeerComparer.Compare(a[i].Key, b[j].Key);
            if (cmp == 0)
            {
                result[k++] = new KeyValuePair<string, ulong>(a[i].Key, Math.Max(a[i].Value, b[j].Value));
                i++;
                j++;
            }
            else if (cmp < 0)
            {
                result[k++] = a[i++];
            }
            else
            {
                result[k++] = b[j++];
            }
        }

        while (i < a.Length) result[k++] = a[i++];
        while (j < b.Length) result[k++] = b[j++];

        if (k < result.Length)
        {
            Array.Resize(ref result, k);
        }

        return new VectorClock(result);
    }

    /// <summary>
    /// Compares two vector clocks according to the strict partial order of distributed causality.
    /// Uses an allocation-free, cache-friendly two-pointer scan over canonical sorted arrays (§3 Step 2).
    /// </summary>
    /// <param name="other">The vector clock to compare against.</param>
    /// <returns>The <see cref="CausalRelation"/> describing the relationship of this clock relative to <paramref name="other"/>.</returns>
    public CausalRelation Compare(VectorClock other)
    {
        ArgumentNullException.ThrowIfNull(other);

        if (ReferenceEquals(this, other))
        {
            return CausalRelation.Equal;
        }

        var a = _entries;
        var b = other._entries;

        if (a.Length == 0 && b.Length == 0) return CausalRelation.Equal;
        if (a.Length == 0) return CausalRelation.Before;
        if (b.Length == 0) return CausalRelation.After;

        int i = 0;
        int j = 0;
        bool v1Greater = false;
        bool v2Greater = false;

        while (i < a.Length && j < b.Length)
        {
            int cmp = PeerComparer.Compare(a[i].Key, b[j].Key);
            if (cmp == 0)
            {
                ulong c1 = a[i].Value;
                ulong c2 = b[j].Value;

                if (c1 > c2) v1Greater = true;
                if (c2 > c1) v2Greater = true;

                i++;
                j++;
            }
            else if (cmp < 0)
            {
                // Key present in a only => c1 > 0, c2 = 0
                v1Greater = true;
                i++;
            }
            else
            {
                // Key present in b only => c1 = 0, c2 > 0
                v2Greater = true;
                j++;
            }

            if (v1Greater && v2Greater)
            {
                return CausalRelation.Concurrent; // Early exit: V1 || V2
            }
        }

        if (i < a.Length) v1Greater = true;
        if (j < b.Length) v2Greater = true;

        if (v1Greater && v2Greater) return CausalRelation.Concurrent;
        if (v1Greater && !v2Greater) return CausalRelation.After;
        if (v2Greater && !v1Greater) return CausalRelation.Before;
        return CausalRelation.Equal;
    }

    /// <summary>
    /// Returns true if this vector clock dominates or equals <paramref name="other"/> (this &gt;= other).
    /// </summary>
    public bool IsDominating(VectorClock other)
    {
        var rel = Compare(other);
        return rel is CausalRelation.Equal or CausalRelation.After;
    }

    /// <summary>
    /// Returns true if this vector clock is dominated by or equals <paramref name="other"/> (this &lt;= other).
    /// </summary>
    public bool IsDominatedBy(VectorClock other)
    {
        var rel = Compare(other);
        return rel is CausalRelation.Equal or CausalRelation.Before;
    }

    /// <summary>
    /// Returns true if this vector clock is causally concurrent / divergent with <paramref name="other"/> (this || other).
    /// </summary>
    public bool IsConcurrentWith(VectorClock other)
    {
        return Compare(other) == CausalRelation.Concurrent;
    }

    // --- Relational Operators ---

    public static bool operator ==(VectorClock? left, VectorClock? right)
    {
        if (ReferenceEquals(left, right)) return true;
        if (left is null || right is null) return false;
        return left.Compare(right) == CausalRelation.Equal;
    }

    public static bool operator !=(VectorClock? left, VectorClock? right)
    {
        return !(left == right);
    }

    public static bool operator <(VectorClock left, VectorClock right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return left.Compare(right) == CausalRelation.Before;
    }

    public static bool operator <=(VectorClock left, VectorClock right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return left.IsDominatedBy(right);
    }

    public static bool operator >(VectorClock left, VectorClock right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return left.Compare(right) == CausalRelation.After;
    }

    public static bool operator >=(VectorClock left, VectorClock right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return left.IsDominating(right);
    }

    // --- Serialization ---

    /// <summary>
    /// Serializes this vector clock to a standard compact JSON string.
    /// </summary>
    public string ToJson()
    {
        return JsonSerializer.Serialize(this);
    }

    /// <summary>
    /// Deserializes a vector clock from a JSON string.
    /// </summary>
    public static VectorClock FromJson(string json)
    {
        ArgumentNullException.ThrowIfNull(json);
        return JsonSerializer.Deserialize<VectorClock>(json) ?? Empty;
    }

    /// <summary>
    /// Serializes this vector clock into a compact binary format.
    /// Format: [ushort entryCount] followed by each entry: [ushort utf8Len][utf8Bytes][ulong counter].
    /// </summary>
    public byte[] ToByteArray()
    {
        if (IsEmpty)
        {
            return Array.Empty<byte>();
        }

        int totalBytes = 2; // entry count
        var encodedKeys = new byte[_entries.Length][];

        for (int i = 0; i < _entries.Length; i++)
        {
            encodedKeys[i] = Encoding.UTF8.GetBytes(_entries[i].Key);
            checked
            {
                totalBytes += 2 + encodedKeys[i].Length + 8;
            }
        }

        var buffer = new byte[totalBytes];
        var span = buffer.AsSpan();

        BinaryPrimitives.WriteUInt16LittleEndian(span[..2], (ushort)_entries.Length);
        int offset = 2;

        for (int i = 0; i < _entries.Length; i++)
        {
            byte[] keyBytes = encodedKeys[i];
            BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(offset, 2), (ushort)keyBytes.Length);
            offset += 2;

            keyBytes.CopyTo(span.Slice(offset, keyBytes.Length));
            offset += keyBytes.Length;

            BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(offset, 8), _entries[i].Value);
            offset += 8;
        }

        return buffer;
    }

    /// <summary>
    /// Deserializes a vector clock from a compact binary buffer.
    /// </summary>
    public static VectorClock FromByteArray(ReadOnlySpan<byte> span)
    {
        if (span.IsEmpty)
        {
            return Empty;
        }

        if (span.Length < 2)
        {
            throw new FormatException("Invalid VectorClock binary buffer: insufficient length.");
        }

        ushort count = BinaryPrimitives.ReadUInt16LittleEndian(span[..2]);
        int offset = 2;

        var entries = new List<KeyValuePair<string, ulong>>(count);

        for (int i = 0; i < count; i++)
        {
            if (span.Length - offset < 2)
            {
                throw new FormatException($"Invalid VectorClock binary buffer at entry {i}: truncated key length.");
            }

            ushort keyLength = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(offset, 2));
            offset += 2;

            if (span.Length - offset < keyLength + 8)
            {
                throw new FormatException($"Invalid VectorClock binary buffer at entry {i}: truncated payload.");
            }

            string key = Encoding.UTF8.GetString(span.Slice(offset, keyLength));
            offset += keyLength;

            ulong counter = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(offset, 8));
            offset += 8;

            if (counter > 0)
            {
                entries.Add(new KeyValuePair<string, ulong>(ValidateAndNormalizePeerId(key), counter));
            }
        }

        return Create(entries);
    }

    // --- Object overrides ---

    public override bool Equals(object? obj)
    {
        return obj is VectorClock other && Equals(other);
    }

    public bool Equals(VectorClock? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        return Compare(other) == CausalRelation.Equal;
    }

    public override int GetHashCode()
    {
        int hash = 0;
        for (int i = 0; i < _entries.Length; i++)
        {
            int entryHash = HashCode.Combine(PeerComparer.GetHashCode(_entries[i].Key), _entries[i].Value);
            hash ^= entryHash;
        }
        return hash;
    }

    public override string ToString()
    {
        if (IsEmpty)
        {
            return "{}";
        }

        var sb = new StringBuilder("{");
        for (int i = 0; i < _entries.Length; i++)
        {
            if (i > 0) sb.Append(", ");
            sb.Append(_entries[i].Key).Append(": ").Append(_entries[i].Value);
        }
        sb.Append('}');
        return sb.ToString();
    }

    // --- IReadOnlyDictionary implementation ---

    public bool ContainsKey(string key)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        return BinarySearch(key.Trim()) >= 0;
    }

    public bool TryGetValue(string key, out ulong value)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            value = 0UL;
            return false;
        }

        int idx = BinarySearch(key.Trim());
        if (idx >= 0)
        {
            value = _entries[idx].Value;
            return true;
        }

        value = 0UL;
        return false;
    }

    public IEnumerator<KeyValuePair<string, ulong>> GetEnumerator()
    {
        return ((IEnumerable<KeyValuePair<string, ulong>>)_entries).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    private int BinarySearch(string key)
    {
        int low = 0;
        int high = _entries.Length - 1;

        while (low <= high)
        {
            int mid = (low + high) >>> 1;
            int cmp = PeerComparer.Compare(_entries[mid].Key, key);

            if (cmp < 0)
            {
                low = mid + 1;
            }
            else if (cmp > 0)
            {
                high = mid - 1;
            }
            else
            {
                return mid;
            }
        }

        return ~low;
    }

    private static string ValidateAndNormalizePeerId(string peerId)
    {
        if (string.IsNullOrWhiteSpace(peerId))
        {
            throw new ArgumentException("Peer ID cannot be null, empty, or whitespace.", nameof(peerId));
        }

        return peerId.Trim();
    }
}
