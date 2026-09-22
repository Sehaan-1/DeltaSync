# Check Verification: Ticket #9 (FastCDC Boundary Scanner)

- **Ticket:** [#9 Implement FastCDC rolling Gear hash and boundary scanner](https://github.com/Sehaan-1/DeltaSync/issues/9)
- **Commit SHA:** `8cd3ce8`
- **Execution Date:** 2026-09-22T21:46:25+05:30
- **Runner Environment:** Windows 11 x64, .NET SDK 8.0.425, Release Configuration

---

## 1. Check Criteria

1. **Property-Based Invariant Check (FsCheck):**
   - Run FsCheck over 1,000 arbitrary byte arrays ($0 \le N \le 10\text{ MB}$).
   - Verify $\sum_{j=1}^m |C_j| = N$.
   - Verify $\forall j < m: 16,384 \le |C_j| \le 262,144$.
   - Verify $1 \le |C_m| \le 262,144$.
2. **Throughput & Zero-Allocation Check:**
   - Scan 50MB synthetic buffer.
   - Assert $\ge 400\text{ MB/s}$ throughput.
   - Assert **0 managed heap allocations** during scanning.

---

## 2. Terminal Transcript

```console
PS C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync> & "C:\Users\INDIA TECHNOLOGY\.dotnet\dotnet.exe" test -c Release --logger "console;verbosity=normal"
  Determining projects to restore...
  All projects are up-to-date for restore.
  DeltaSync.Core -> C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\src\DeltaSync.Core\bin\Release\net8.0\DeltaSync.Core.dll
  DeltaSync.Network -> C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\src\DeltaSync.Network\bin\Release\net8.0\DeltaSync.Network.dll
  DeltaSync.Tests -> C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\tests\DeltaSync.Tests\bin\Release\net8.0\DeltaSync.Tests.dll
Test run for C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\tests\DeltaSync.Tests\bin\Release\net8.0\DeltaSync.Tests.dll (.NETCoreApp,Version=v8.0)
VSTest version 17.11.1 (x64)

Starting test execution, please wait...
A total of 1 test files matched the specified pattern.
[xUnit.net 00:00:00.00] xUnit.net VSTest Adapter v2.8.2+699d445a1a (64-bit .NET 8.0.31)
[xUnit.net 00:00:00.13]   Discovering: DeltaSync.Tests
[xUnit.net 00:00:00.21]   Discovered:  DeltaSync.Tests
[xUnit.net 00:00:00.21]   Starting:    DeltaSync.Tests
  Passed DeltaSync.Tests.Chunking.FastCdcChunkerTests.BufferEqualToMinSize_YieldsSingleChunk [20 ms]
  Passed DeltaSync.Tests.CoreHarnessTests.FileMetadata_CanBeInstantiated_WithValidProperties [21 ms]
  Passed DeltaSync.Tests.Chunking.FastCdcChunkerTests.FsCheck_RandomPayloads_SatisfySizeBoundsAndExactSum [609 ms]
  Passed DeltaSync.Tests.Chunking.FastCdcChunkerTests.Deterministic_SameInput_ProducesIdenticalPartition [7 ms]
  Passed DeltaSync.Tests.Chunking.FastCdcChunkerTests.BufferSmallerThanMinSize_YieldsSingleChunk(length: 16383) [< 1 ms]
  Passed DeltaSync.Tests.Chunking.FastCdcChunkerTests.BufferSmallerThanMinSize_YieldsSingleChunk(length: 100) [< 1 ms]
  Passed DeltaSync.Tests.Chunking.FastCdcChunkerTests.BufferSmallerThanMinSize_YieldsSingleChunk(length: 1) [< 1 ms]
  Passed DeltaSync.Tests.Chunking.FastCdcChunkerTests.BufferSmallerThanMinSize_YieldsSingleChunk(length: 1024) [< 1 ms]
  Passed DeltaSync.Tests.Chunking.FastCdcChunkerTests.Config_ThrowsOnInvalidParameters [6 ms]
FastCDC 50MB Scan: 1479.73 MB/s in 33.8 ms (700 chunks, 0 bytes allocated)
[xUnit.net 00:00:01.29]   Finished:    DeltaSync.Tests
  Passed DeltaSync.Tests.Chunking.FastCdcChunkerTests.EmptyBuffer_YieldsZeroChunks [2 ms]
  Passed DeltaSync.Tests.Chunking.FastCdcChunkerTests.RepetitiveZeros_EnforcesMaxSizeClamp [2 ms]
  Passed DeltaSync.Tests.Chunking.FastCdcChunkerTests.Benchmark_ThroughputExceeds400MBps_ZeroAllocations [255 ms]
  Passed DeltaSync.Tests.Chunking.FastCdcChunkerTests.LargeBuffer_10MB_SatisfiesAllInvariants [48 ms]

Test Run Successful.
Total tests: 13
     Passed: 13
 Total time: 1.9039 Seconds
```

---

## 3. Results Summary

| Assertion | Required Bar | Measured Value | Status |
|---|---|---|---|
| FsCheck random payloads (1,000 iterations) | 1,000 passes, size bounds $[16\text{KB}, 256\text{KB}]$, exact byte sum | 1,000 passed in 609 ms | **PASS** |
| Hot scanning loop memory allocation | $0\text{ bytes}$ | **0 bytes** | **PASS** |
| 50MB synthetic buffer scanning throughput | $\ge 400.00\text{ MB/s}$ | **1,479.73 MB/s** (in 33.8 ms) | **PASS** |
| Full 10MB payload chunking verification | Completeness and bounds | Passed in 48 ms | **PASS** |
| Micro & edge buffer chunking | Exact length chunk | Passed (<1 ms) | **PASS** |
| Zero buffer chunking | 0 chunks | Passed (2 ms) | **PASS** |

**Conclusion:** Ticket #9 Check PASSED.
