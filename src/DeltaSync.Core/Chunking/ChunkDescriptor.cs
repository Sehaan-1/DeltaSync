namespace DeltaSync.Core.Chunking;

/// <summary>
/// Immutable descriptor of a content-defined chunk, containing its monotonic index,
/// byte offset, length, and cryptographic SHA-256 digest.
/// </summary>
public sealed record ChunkDescriptor : IEquatable<ChunkDescriptor>
{
    public int Index { get; }
    public long Offset { get; }
    public int Length { get; }
    public byte[] Hash { get; }

    /// <summary>
    /// Hexadecimal string representation of the 32-byte SHA-256 chunk hash in lowercase.
    /// </summary>
    public string HashHex => Convert.ToHexString(Hash).ToLowerInvariant();

    public ChunkDescriptor(int index, long offset, int length, byte[] hash)
    {
        if (index < 0)
            throw new ArgumentOutOfRangeException(nameof(index), "Index must be non-negative.");
        if (offset < 0)
            throw new ArgumentOutOfRangeException(nameof(offset), "Offset must be non-negative.");
        if (length < 0)
            throw new ArgumentOutOfRangeException(nameof(length), "Length must be non-negative.");
        ArgumentNullException.ThrowIfNull(hash);
        if (hash.Length != 32)
            throw new ArgumentException("Chunk hash must be a 32-byte SHA-256 digest.", nameof(hash));

        Index = index;
        Offset = offset;
        Length = length;
        Hash = (byte[])hash.Clone();
    }

    public bool Equals(ChunkDescriptor? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return Index == other.Index &&
               Offset == other.Offset &&
               Length == other.Length &&
               Hash.AsSpan().SequenceEqual(other.Hash.AsSpan());
    }

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Index);
        hash.Add(Offset);
        hash.Add(Length);
        if (Hash.Length >= 4)
        {
            hash.Add(BitConverter.ToInt32(Hash, 0));
        }
        return hash.ToHashCode();
    }

    public override string ToString() => $"Chunk[{Index}]: Offset={Offset}, Length={Length}, Hash={HashHex}";
}
