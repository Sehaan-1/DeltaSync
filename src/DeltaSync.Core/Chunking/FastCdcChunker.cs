namespace DeltaSync.Core.Chunking;

/// <summary>
/// Fast Content-Defined Chunking (FastCDC) boundary scanner.
/// Implements gear rolling hashing, sub-minimum skipping, and dual-mask normalization.
/// </summary>
public static class FastCdcChunker
{
    /// <summary>
    /// Identifies the next chunk cut-point within a byte slice.
    /// Returns the length of the identified chunk in bytes.
    /// </summary>
    public static int NextChunk(ReadOnlySpan<byte> data, FastCdcConfig? config = null)
    {
        var cfg = config ?? FastCdcConfig.Default;
        int rem = data.Length;
        if (rem <= cfg.MinSize)
        {
            return rem;
        }

        int targetLimit = Math.Min(cfg.TargetSize, rem);
        int maxLimit = Math.Min(cfg.MaxSize, rem);

        ulong h = 0;
        int i = cfg.MinSize;

        ulong[] gear = GearTable.Table;
        ulong maskS = cfg.MaskS;
        ulong maskL = cfg.MaskL;

        // Small region: [MinSize, TargetSize)
        while (i < targetLimit)
        {
            h = (h << 1) + gear[data[i]];
            if ((h & maskS) == 0)
            {
                return i + 1;
            }
            i++;
        }

        // Large region: [TargetSize, MaxSize)
        while (i < maxLimit)
        {
            h = (h << 1) + gear[data[i]];
            if ((h & maskL) == 0)
            {
                return i + 1;
            }
            i++;
        }

        // Hard clamp at MaxSize or end of available stream
        return maxLimit;
    }

    /// <summary>
    /// Returns a zero-allocation ref struct enumerator over a byte span.
    /// </summary>
    public static FastCdcSpanEnumerator EnumerateChunks(ReadOnlySpan<byte> data, FastCdcConfig? config = null)
    {
        return new FastCdcSpanEnumerator(data, config ?? FastCdcConfig.Default);
    }

    /// <summary>
    /// Partitions an in-memory byte buffer into a list of ChunkSlices.
    /// </summary>
    public static List<ChunkSlice> Chunk(ReadOnlyMemory<byte> data, FastCdcConfig? config = null)
    {
        var cfg = config ?? FastCdcConfig.Default;
        var list = new List<ChunkSlice>();
        var span = data.Span;
        long globalOffset = 0;

        while (span.Length > 0)
        {
            int chunkLen = NextChunk(span, cfg);
            list.Add(new ChunkSlice(globalOffset, chunkLen));
            globalOffset += chunkLen;
            span = span.Slice(chunkLen);
        }

        return list;
    }
}

/// <summary>
/// High-performance zero-allocation ref struct enumerator for FastCDC chunk boundaries.
/// </summary>
public ref struct FastCdcSpanEnumerator
{
    private ReadOnlySpan<byte> _remaining;
    private readonly FastCdcConfig _config;
    private long _currentOffset;
    private ChunkSlice _current;

    public FastCdcSpanEnumerator(ReadOnlySpan<byte> data, FastCdcConfig config)
    {
        _remaining = data;
        _config = config;
        _currentOffset = 0;
        _current = default;
    }

    public readonly ChunkSlice Current => _current;

    public bool MoveNext()
    {
        if (_remaining.IsEmpty)
        {
            return false;
        }

        int chunkLen = FastCdcChunker.NextChunk(_remaining, _config);
        _current = new ChunkSlice(_currentOffset, chunkLen);
        _currentOffset += chunkLen;
        _remaining = _remaining.Slice(chunkLen);
        return true;
    }

    public readonly FastCdcSpanEnumerator GetEnumerator() => this;
}
