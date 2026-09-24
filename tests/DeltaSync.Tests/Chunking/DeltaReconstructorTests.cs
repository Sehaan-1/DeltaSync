using System.Security.Cryptography;
using System.Text;
using DeltaSync.Core.Chunking;
using Xunit;

namespace DeltaSync.Tests.Chunking;

public class DeltaReconstructorTests : IDisposable
{
    private readonly string _testRoot;

    public DeltaReconstructorTests()
    {
        _testRoot = Path.Combine(Path.GetTempPath(), "deltasync-reconstruct-tests-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_testRoot);
    }

    public void Dispose()
    {
        if (Directory.Exists(_testRoot))
        {
            try
            {
                Directory.Delete(_testRoot, recursive: true);
            }
            catch
            {
                // Best-effort cleanup
            }
        }
    }

    /// <summary>
    /// Spec §10 Check 3 & Ticket #11 Check 1:
    /// Create 10MB test file F1.
    /// Create F2 by editing 3 lines and prepending 100 bytes to F1.
    /// Execute delta reconstruction on F2 using local chunks from F1 and remote missing chunks.
    /// Assert reconstructed F2 matches original F2 bit-for-bit (SHA256(F2_reconstructed) == SHA256(F2)).
    /// Assert total transmitted bytes <= 192 KB (>98% bandwidth savings).
    /// </summary>
    [Fact]
    public async Task SpecSection10_Check3_DeltaReconstructionEquivalence_RetainsOver98PercentSavings()
    {
        // 1. Generate 10MB test file F1
        int f1Size = 10 * 1024 * 1024; // 10,485,760 bytes
        byte[] f1Bytes = new byte[f1Size];
        var rng = new Random(42); // Seeded for reproducibility
        rng.NextBytes(f1Bytes);

        string f1Path = Path.Combine(_testRoot, "f1.bin");
        await File.WriteAllBytesAsync(f1Path, f1Bytes);
        var f1Manifest = ChunkFingerprinter.CreateManifest("f1.bin", f1Bytes);

        // 2. Generate F2 by prepending 100 bytes and modifying 3 lines in the middle
        byte[] prependBytes = new byte[100];
        rng.NextBytes(prependBytes);

        byte[] f2Bytes = new byte[100 + f1Size];
        Buffer.BlockCopy(prependBytes, 0, f2Bytes, 0, 100);
        Buffer.BlockCopy(f1Bytes, 0, f2Bytes, 100, f1Size);

        // Edit 3 lines in a localized block in the middle of the file
        int editOffset = 100 + 5 * 1024 * 1024;
        f2Bytes[editOffset] ^= 0xAA;
        f2Bytes[editOffset + 60] ^= 0x55;
        f2Bytes[editOffset + 120] ^= 0xFF;

        string f2Path = Path.Combine(_testRoot, "f2.bin");
        await File.WriteAllBytesAsync(f2Path, f2Bytes);
        var f2Manifest = ChunkFingerprinter.CreateManifest("f2.bin", f2Bytes);

        // 3. Compute manifest diff
        var diff = ChunkFingerprinter.ComputeDiff(f2Manifest, f1Manifest);

        // 4. Set up local chunk provider backed by F1 file and remote source for missing chunks
        using var localProvider = new FileChunkProvider(f1Path, f1Manifest);
        var remoteSource = new MemoryRemoteChunkSource();

        // Populate remote source ONLY with missing chunks from F2
        foreach (var missingChunk in diff.MissingChunks)
        {
            byte[] missingData = f2Bytes.AsSpan((int)missingChunk.Offset, missingChunk.Length).ToArray();
            remoteSource.AddChunk(missingChunk.HashHex, missingData);
        }

        // 5. Execute delta reconstruction
        string reconstructedF2Path = Path.Combine(_testRoot, "f2_reconstructed.bin");
        var result = await DeltaReconstructor.ReconstructAsync(
            f2Manifest,
            reconstructedF2Path,
            localProvider,
            remoteSource);

        // 6. Assertions
        Assert.True(File.Exists(reconstructedF2Path));
        byte[] reconstructedBytes = await File.ReadAllBytesAsync(reconstructedF2Path);

        // Bit-for-bit whole-file SHA-256 equivalence
        byte[] expectedHash = SHA256.HashData(f2Bytes);
        byte[] actualHash = SHA256.HashData(reconstructedBytes);
        Assert.True(expectedHash.AsSpan().SequenceEqual(actualHash));
        Assert.Equal(f2Manifest.RootHash, result.RootHash);

        // Bandwidth savings assertion: Transmitted bytes <= 192 KB (>98% savings)
        Assert.InRange(result.TransmittedBytes, 1, 192 * 1024);
        Assert.True(result.BandwidthSavingsRatio > 0.98,
            $"Expected > 98% bandwidth savings, but measured {result.BandwidthSavingsRatio * 100:F2}% (Transmitted: {result.TransmittedBytes} bytes / {result.TotalBytes} bytes).");
        Assert.True(result.ReusedBytes > 0);
        Assert.Equal(f2Bytes.Length, result.TotalBytes);
    }

    /// <summary>
    /// Crash & Corruption Abort Check (Ticket #11 Check 2):
    /// Inject a single bit-flip into one transmitted chunk.
    /// Verify that reconstructor throws ChunkIntegrityException,
    /// the temporary file is deleted, and the original destination file remains completely unaltered.
    /// </summary>
    [Fact]
    public async Task CrashAndCorruption_BitFlipInTransmittedChunk_AbortsImmediatelyAndPreservesOriginalFile()
    {
        // Destination starts with existing legitimate file content
        string destinationPath = Path.Combine(_testRoot, "important_document.txt");
        byte[] originalContent = Encoding.UTF8.GetBytes("Original pristine destination file content that must NEVER be corrupted.");
        await File.WriteAllBytesAsync(destinationPath, originalContent);
        byte[] originalHash = SHA256.HashData(originalContent);

        // Target file to reconstruct
        byte[] targetData = new byte[70_000];
        Random.Shared.NextBytes(targetData);
        var targetManifest = ChunkFingerprinter.CreateManifest("important_document.txt", targetData);

        var emptyLocalProvider = new MemoryChunkProvider();
        var corruptRemoteSource = new MemoryRemoteChunkSource();

        // Inject corrupt bit-flip into chunk 0
        for (int i = 0; i < targetManifest.Chunks.Count; i++)
        {
            var chunk = targetManifest.Chunks[i];
            byte[] chunkBytes = targetData.AsSpan((int)chunk.Offset, chunk.Length).ToArray();
            if (i == 0)
            {
                // Corrupt chunk 0 with a bit-flip
                chunkBytes[0] ^= 0x01;
            }
            corruptRemoteSource.AddChunk(chunk.HashHex, chunkBytes);
        }

        string tempDir = Path.Combine(_testRoot, ".deltasync", "tmp");

        // Act & Assert: throws ChunkIntegrityException
        var ex = await Assert.ThrowsAsync<ChunkIntegrityException>(() =>
            DeltaReconstructor.ReconstructAsync(
                targetManifest,
                destinationPath,
                emptyLocalProvider,
                corruptRemoteSource,
                tempDirectory: tempDir));

        Assert.Equal(0, ex.ChunkIndex);

        // Verify: temporary file is deleted
        if (Directory.Exists(tempDir))
        {
            var remainingTempFiles = Directory.GetFiles(tempDir, "*.tmp");
            Assert.Empty(remainingTempFiles);
        }

        // Verify: original destination file is 100% unaltered bit-for-bit
        Assert.True(File.Exists(destinationPath));
        byte[] currentDestBytes = await File.ReadAllBytesAsync(destinationPath);
        Assert.True(originalHash.AsSpan().SequenceEqual(SHA256.HashData(currentDestBytes)));
        Assert.Equal(originalContent, currentDestBytes);
    }

    [Fact]
    public async Task RootHashMismatch_WhenIndividualChunksPass_ThrowsChunkIntegrityExceptionAndDeletesTempFile()
    {
        string destinationPath = Path.Combine(_testRoot, "root_tamper.txt");
        byte[] payload = [1, 2, 3, 4, 5];
        var realManifest = ChunkFingerprinter.CreateManifest("root_tamper.txt", payload);

        // Maliciously forged manifest with wrong root hash
        string fakeRootHash = "0000000000000000000000000000000000000000000000000000000000000000";
        var forgedManifest = new FileManifest("root_tamper.txt", payload.Length, fakeRootHash, realManifest.Chunks);

        var localProvider = new MemoryChunkProvider();
        localProvider.AddChunk(realManifest.Chunks[0].HashHex, payload);
        var remoteSource = new MemoryRemoteChunkSource();

        string tempDir = Path.Combine(_testRoot, ".deltasync", "tmp");

        var ex = await Assert.ThrowsAsync<ChunkIntegrityException>(() =>
            DeltaReconstructor.ReconstructAsync(
                forgedManifest,
                destinationPath,
                localProvider,
                remoteSource,
                tempDirectory: tempDir));

        Assert.Null(ex.ChunkIndex);
        Assert.Contains("Whole-file root hash", ex.Message);
        Assert.False(File.Exists(destinationPath));

        if (Directory.Exists(tempDir))
        {
            Assert.Empty(Directory.GetFiles(tempDir, "*.tmp"));
        }
    }

    [Fact]
    public async Task EmptyFile_ReconstructsZeroByteFileSuccessfully()
    {
        string destPath = Path.Combine(_testRoot, "empty_reconstructed.bin");
        var emptyManifest = FileManifest.CreateEmpty("empty.bin");

        var localProvider = new MemoryChunkProvider();
        var remoteSource = new MemoryRemoteChunkSource();

        var result = await DeltaReconstructor.ReconstructAsync(
            emptyManifest,
            destPath,
            localProvider,
            remoteSource);

        Assert.True(File.Exists(destPath));
        Assert.Equal(0, new FileInfo(destPath).Length);
        Assert.Equal(0, result.TotalBytes);
        Assert.Equal(FileManifest.EmptyRootHash, result.RootHash);
        Assert.Equal(1.0, result.ReuseRatio);
        Assert.Equal(1.0, result.BandwidthSavingsRatio);
    }

    [Fact]
    public void SynchronousReconstruct_OperatesIdenticallyToAsync()
    {
        string destPath = Path.Combine(_testRoot, "sync_test.bin");
        byte[] payload = [42, 43, 44, 45, 46];
        var manifest = ChunkFingerprinter.CreateManifest("sync_test.bin", payload);

        var localProvider = new MemoryChunkProvider();
        localProvider.AddChunk(manifest.Chunks[0].HashHex, payload);
        var remoteSource = new MemoryRemoteChunkSource();

#pragma warning disable CS0618
        var result = DeltaReconstructor.Reconstruct(manifest, destPath, localProvider, remoteSource);
#pragma warning restore CS0618

        Assert.True(File.Exists(destPath));
        Assert.Equal(payload, File.ReadAllBytes(destPath));
        Assert.Equal(payload.Length, result.TotalBytes);
        Assert.Equal(payload.Length, result.ReusedBytes);
        Assert.Equal(0, result.TransmittedBytes);
    }
}
