namespace DeltaSync.Core.Chunking;

/// <summary>
/// Precomputed 256-element 64-bit Gear Table for FastCDC rolling hashing.
/// Uses a deterministic SplitMix64 PRNG with a pinned seed for cross-platform determinism.
/// </summary>
public static class GearTable
{
    public static readonly ulong[] Table = InitializeTable();

    private static ulong[] InitializeTable()
    {
        var table = new ulong[256];
        ulong state = 0x20160601UL; // Pinned seed (Xia et al. USENIX ATC 2016)
        for (int i = 0; i < 256; i++)
        {
            state += 0x9E3779B97F4A7C15UL;
            ulong z = state;
            z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9UL;
            z = (z ^ (z >> 27)) * 0x94D049BB133111EBUL;
            table[i] = z ^ (z >> 31);
        }
        return table;
    }
}
