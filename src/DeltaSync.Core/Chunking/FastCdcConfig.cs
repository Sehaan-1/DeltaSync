namespace DeltaSync.Core.Chunking;

/// <summary>
/// Configuration parameters for FastCDC normalized content-defined chunking.
/// Default: Min = 16 KB, Target = 64 KB, Max = 256 KB, Normalization Level = 2.
/// </summary>
public sealed record FastCdcConfig
{
    public int MinSize { get; }
    public int TargetSize { get; }
    public int MaxSize { get; }
    public int NormalizationLevel { get; }

    public ulong MaskS { get; }
    public ulong MaskL { get; }

    public static FastCdcConfig Default { get; } = new(16_384, 65_536, 262_144, 2);

    public FastCdcConfig(int minSize = 16_384, int targetSize = 65_536, int maxSize = 262_144, int normalizationLevel = 2)
    {
        if (minSize < 1)
            throw new ArgumentOutOfRangeException(nameof(minSize), "minSize must be at least 1 byte.");
        if (minSize >= targetSize)
            throw new ArgumentOutOfRangeException(nameof(minSize), "minSize must be strictly less than targetSize.");
        if (targetSize >= maxSize)
            throw new ArgumentOutOfRangeException(nameof(targetSize), "targetSize must be strictly less than maxSize.");
        if (normalizationLevel < 1 || normalizationLevel > 4)
            throw new ArgumentOutOfRangeException(nameof(normalizationLevel), "normalizationLevel must be between 1 and 4.");

        MinSize = minSize;
        TargetSize = targetSize;
        MaxSize = maxSize;
        NormalizationLevel = normalizationLevel;

        int k = (int)Math.Round(Math.Log2(targetSize));
        int bitsS = k + normalizationLevel;
        int bitsL = k - normalizationLevel;

        MaskS = (1UL << bitsS) - 1UL;
        MaskL = (1UL << bitsL) - 1UL;
    }
}
