using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using DeltaSync.Tests.Harness;
using FluentAssertions;
using Xunit;
using Xunit.Abstractions;

namespace DeltaSync.Tests.Sync;

/// <summary>
/// End-to-end multi-process directory synchronization integration tests verifying
/// physical socket communication, FastCDC delta deduplication, and vector-clock conflict preservation
/// between independent operating system processes (Engine 8 Spec §3 Step 4 & §10 Check 3, Issue #39).
/// Honors ADR-0001, ADR-0002, ADR-0004, and ADR-0005.
/// </summary>
public class MultiProcessCliSyncIntegrationTests
{
    private readonly ITestOutputHelper _output;

    public MultiProcessCliSyncIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    /// <summary>
    /// Phase 1 (Spec §3 Step 4.1 & §10 Check 3):
    /// Ingest 10 files in Node A; verify Node B receives and verifies identical SHA-256 hashes within 5.0s
    /// over real physical loopback TCP sockets.
    /// </summary>
    [Fact]
    public async Task MultiProcessSync_Forward10FileBatchSync_ConvergesWithBitForBitSha256Equivalence_Test()
    {
        // Arrange: Launch two CLI daemon instances with fast debounce (50ms)
        var options = new CliHarnessOptions
        {
            StartupTimeout = TimeSpan.FromSeconds(5.0),
            HttpPollInterval = TimeSpan.FromMilliseconds(50),
            DeleteOnDispose = true,
            AutoPeer = true,
            ExtraArgsNodeA = new[] { "--debounce-ms", "50" },
            ExtraArgsNodeB = new[] { "--debounce-ms", "50" }
        };

        await using var harness = await MultiProcessCliHarness.StartAsync(options);
        _output.WriteLine($"Node A (PID {harness.NodeA.Id}) listening on port {harness.NodeA.ListenPort}, metrics on {harness.NodeA.MetricsPort}");
        _output.WriteLine($"Node B (PID {harness.NodeB.Id}) listening on port {harness.NodeB.ListenPort}, metrics on {harness.NodeB.MetricsPort}");

        // Act: Create 10 files on Node A (8 text files, 1 nested subdirectory file, and 1 binary file)
        var expectedHashes = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        for (int i = 1; i <= 8; i++)
        {
            string rel = $"doc_{i}.txt";
            string content = $"Multi-process E2E synchronization test payload file {i}.\n" +
                             $"Created by Node A (PID {harness.NodeA.Id}) at {DateTime.UtcNow:O}.\n" +
                             $"Padding block: {new string('Z', i * 256)}\n";
            byte[] bytes = Encoding.UTF8.GetBytes(content);
            await File.WriteAllBytesAsync(Path.Combine(harness.NodeA.WorkingDirectory, rel), bytes);
            expectedHashes[rel] = Convert.ToHexString(SHA256.HashData(bytes));
        }

        // Nested directory file
        string nestedRel = Path.Combine("docs", "deep", "architecture.md");
        string nestedFullA = Path.Combine(harness.NodeA.WorkingDirectory, nestedRel);
        Directory.CreateDirectory(Path.GetDirectoryName(nestedFullA)!);
        byte[] nestedBytes = Encoding.UTF8.GetBytes("# Multi-Process Synchronization Architecture\nVerified over physical OS TCP sockets.");
        await File.WriteAllBytesAsync(nestedFullA, nestedBytes);
        expectedHashes[nestedRel] = Convert.ToHexString(SHA256.HashData(nestedBytes));

        // Binary file
        string binaryRel = Path.Combine("data", "random.bin");
        string binaryFullA = Path.Combine(harness.NodeA.WorkingDirectory, binaryRel);
        Directory.CreateDirectory(Path.GetDirectoryName(binaryFullA)!);
        byte[] binaryBytes = new byte[32 * 1024]; // 32 KB
        new Random(1337).NextBytes(binaryBytes);
        await File.WriteAllBytesAsync(binaryFullA, binaryBytes);
        expectedHashes[binaryRel] = Convert.ToHexString(SHA256.HashData(binaryBytes));

        expectedHashes.Count.Should().Be(10, "Node A must provision exactly 10 test files.");

        // Assert: Wait for all 10 files to converge on Node B with bit-for-bit identical SHA-256
        int countB = await MultiProcessCliHarness.WaitForFileCountAsync(
            harness.NodeB.WorkingDirectory,
            expectedCount: 10,
            timeout: TimeSpan.FromSeconds(6.0),
            nodeContext: harness.NodeB,
            otherNodeContext: harness.NodeA);
        countB.Should().BeGreaterThanOrEqualTo(10);

        foreach (var (relPath, expectedSha256) in expectedHashes)
        {
            string actualSha256 = await MultiProcessCliHarness.WaitForFileAsync(
                harness.NodeB.WorkingDirectory,
                relPath,
                expectedSha256,
                timeout: TimeSpan.FromSeconds(5.0),
                nodeContext: harness.NodeB,
                otherNodeContext: harness.NodeA);

            actualSha256.Should().Be(expectedSha256, $"File '{relPath}' on Node B must match Node A bit-for-bit.");
        }

        _output.WriteLine("Phase 1 Passed: 10 files converged across physical TCP sockets with 100% SHA-256 equivalence.");
    }

    /// <summary>
    /// Phase 2 (Spec §3 Step 4.2 & §10 Check 3):
    /// In-place edit of 1MB file in Node B; verify Node A delta update with >= 75% FastCDC chunk reuse ratio.
    /// </summary>
    [Fact]
    public async Task MultiProcessSync_ReverseDeltaSync_1MBFile_VerifiesFastCdcChunkReuse_Test()
    {
        // Arrange: Launch two CLI daemon instances
        var options = new CliHarnessOptions
        {
            StartupTimeout = TimeSpan.FromSeconds(5.0),
            HttpPollInterval = TimeSpan.FromMilliseconds(50),
            DeleteOnDispose = true,
            AutoPeer = true,
            ExtraArgsNodeA = new[] { "--debounce-ms", "50" },
            ExtraArgsNodeB = new[] { "--debounce-ms", "50" }
        };

        await using var harness = await MultiProcessCliHarness.StartAsync(options);

        // Step 1: Create 1 MB file on Node A and await sync to Node B
        string largeRel = "large_dataset.bin";
        byte[] largeBytes = new byte[1024 * 1024]; // 1 MB
        new Random(42).NextBytes(largeBytes);
        string initialHashA = Convert.ToHexString(SHA256.HashData(largeBytes));

        await File.WriteAllBytesAsync(Path.Combine(harness.NodeA.WorkingDirectory, largeRel), largeBytes);

        // Await convergence on Node B
        string initialHashB = await MultiProcessCliHarness.WaitForFileAsync(
            harness.NodeB.WorkingDirectory,
            largeRel,
            initialHashA,
            timeout: TimeSpan.FromSeconds(6.0),
            nodeContext: harness.NodeB,
            otherNodeContext: harness.NodeA);
        initialHashB.Should().Be(initialHashA);

        _output.WriteLine("Initial 1 MB file synchronized from Node A to Node B.");

        // Allow 100ms settling time for DB transactions to complete before inducing reverse mutation
        await Task.Delay(100);

        // Step 2: Mutate 16 KB of bytes in the middle (offset 500 KB) on Node B (Reverse delta: Node B -> Node A)
        byte[] mutatedBytes = (byte[])largeBytes.Clone();
        for (int b = 0; b < 16 * 1024; b++)
        {
            mutatedBytes[500 * 1024 + b] ^= 0x5A;
        }
        string mutatedHashB = Convert.ToHexString(SHA256.HashData(mutatedBytes));
        mutatedHashB.Should().NotBe(initialHashA, "Mutated hash must differ from initial content.");

        await File.WriteAllBytesAsync(Path.Combine(harness.NodeB.WorkingDirectory, largeRel), mutatedBytes);

        // Step 3: Await reverse delta sync from Node B to Node A
        string updatedHashA = await MultiProcessCliHarness.WaitForFileAsync(
            harness.NodeA.WorkingDirectory,
            largeRel,
            mutatedHashB,
            timeout: TimeSpan.FromSeconds(6.0),
            nodeContext: harness.NodeA,
            otherNodeContext: harness.NodeB);
        updatedHashA.Should().Be(mutatedHashB, "Node A must converge to mutated 1MB file with identical SHA-256.");

        // Step 4: Verify FastCDC chunk deduplication ratio >= 75% on Node A
        long? deduplicatedBytes = await harness.NodeA.GetMetricValueAsync("deltasync_chunks_deduplicated_total");
        _output.WriteLine($"Node A deduplicated bytes reported: {deduplicatedBytes ?? 0}");

        deduplicatedBytes.Should().NotBeNull("Node A metrics endpoint must expose deltasync_chunks_deduplicated_total.");
        deduplicatedBytes!.Value.Should().BeGreaterThanOrEqualTo(750_000,
            "FastCDC must reuse at least 75% (>= 750,000 bytes) of unmodified chunks from the 1 MB file.");

        _output.WriteLine("Phase 2 Passed: 1 MB reverse delta update succeeded with FastCDC chunk reuse >= 75%.");
    }

    /// <summary>
    /// Phase 3 (Spec §3 Step 4.3 & §10 Check 3):
    /// Induce simultaneous concurrent edit; verify .sync-conflict-* side-by-side branch files created on both nodes
    /// with zero data loss (ADR-0001).
    /// </summary>
    [Fact]
    public async Task MultiProcessSync_ConcurrentEdits_PreservesSideBySideConflictFiles_WithZeroDataLoss_Test()
    {
        // Arrange: Launch two CLI daemon instances.
        // Use a 1 000 ms debounce so both nodes fully ingest their concurrent local edits
        // before the first cross-node sync fires. With a 50 ms debounce the watcher on
        // Node A fires and pushes to Node B before Node B's own write is ingested, making
        // Node B see a sequential ApplyRemote rather than a Concurrent (conflict) clock pair.
        var options = new CliHarnessOptions
        {
            StartupTimeout = TimeSpan.FromSeconds(5.0),
            HttpPollInterval = TimeSpan.FromMilliseconds(50),
            DeleteOnDispose = true,
            AutoPeer = true,
            ExtraArgsNodeA = new[] { "--debounce-ms", "1000" },
            ExtraArgsNodeB = new[] { "--debounce-ms", "1000" }
        };

        await using var harness = await MultiProcessCliHarness.StartAsync(options);

        // Step 1: Create baseline contract file on Node A and await sync to Node B
        string docRel = "contract.txt";
        string baselineContent = "Baseline agreement between parties. Validated initial version.";
        byte[] baselineBytes = Encoding.UTF8.GetBytes(baselineContent);
        string baselineSha = Convert.ToHexString(SHA256.HashData(baselineBytes));

        await File.WriteAllBytesAsync(Path.Combine(harness.NodeA.WorkingDirectory, docRel), baselineBytes);

        await MultiProcessCliHarness.WaitForFileAsync(
            harness.NodeB.WorkingDirectory,
            docRel,
            baselineSha,
            timeout: TimeSpan.FromSeconds(6.0),
            nodeContext: harness.NodeB,
            otherNodeContext: harness.NodeA);

        _output.WriteLine("Baseline contract file converged on both nodes.");

        // Step 2: Induce simultaneous concurrent offline edits on both nodes
        string textA = "Revision A from Node A: Critical financial transaction terms added.";
        string textB = "Revision B from Node B: Legal liability indemnification clause added.";

        await File.WriteAllTextAsync(Path.Combine(harness.NodeA.WorkingDirectory, docRel), textA);
        await File.WriteAllTextAsync(Path.Combine(harness.NodeB.WorkingDirectory, docRel), textB);

        // Step 3: Await synchronization settling and conflict resolution
        var dirA = new DirectoryInfo(harness.NodeA.WorkingDirectory);
        var dirB = new DirectoryInfo(harness.NodeB.WorkingDirectory);

        var sw = Stopwatch.StartNew();
        List<FileInfo> conflictsA = new();
        List<FileInfo> conflictsB = new();

        // Allow 12 s: 1 000 ms debounce + ingest + network round-trip + convergence margin
        while (sw.Elapsed < TimeSpan.FromSeconds(12.0))
        {
            conflictsA = dirA.EnumerateFiles("*conflict*", SearchOption.TopDirectoryOnly).ToList();
            conflictsB = dirB.EnumerateFiles("*conflict*", SearchOption.TopDirectoryOnly).ToList();

            if (conflictsA.Count >= 1 && conflictsB.Count >= 1)
            {
                break;
            }
            await Task.Delay(100);
        }
        // Assert: ADR-0001 Side-by-side conflict files preserved
        conflictsA.Should().HaveCount(1, $"Node A must contain exactly 1 conflict file. Output: {harness.NodeA.GetAllOutput()}");
        conflictsB.Should().HaveCount(1, $"Node B must contain exactly 1 conflict file. Output: {harness.NodeB.GetAllOutput()}");

        string conflictNameA = conflictsA[0].Name;
        string conflictNameB = conflictsB[0].Name;
        conflictNameA.Should().Be(conflictNameB, "Both nodes must generate identical deterministic conflict file names.");

        // Assert: Read both files on both nodes
        string primaryA = await File.ReadAllTextAsync(Path.Combine(harness.NodeA.WorkingDirectory, docRel));
        string primaryB = await File.ReadAllTextAsync(Path.Combine(harness.NodeB.WorkingDirectory, docRel));
        primaryA.Should().Be(primaryB, "Primary contract file must converge to identical content across both nodes.");

        string conflictContentA = await File.ReadAllTextAsync(conflictsA[0].FullName);
        string conflictContentB = await File.ReadAllTextAsync(conflictsB[0].FullName);
        conflictContentA.Should().Be(conflictContentB, "Conflict branch file must converge to identical content across both nodes.");

        // Assert: Zero data loss (both revisions exist preserved)
        var allContents = new HashSet<string> { primaryA, conflictContentA };
        allContents.Should().Contain(textA, "Revision A must be preserved without data loss.");
        allContents.Should().Contain(textB, "Revision B must be preserved without data loss.");

        _output.WriteLine("Phase 3 Passed: Concurrent conflict branched side-by-side with zero data loss (ADR-0001).");
    }
}
