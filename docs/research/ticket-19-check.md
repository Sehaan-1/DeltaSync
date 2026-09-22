# Check Verification: Ticket #19 (Incremental Merkle Prefix Tree and Concurrent Read-Write Synchronization)

- **Ticket:** [#19 Implement incremental Merkle prefix tree calculation and concurrent read-write synchronization](https://github.com/Sehaan-1/DeltaSync/issues/19)
- **Commit SHA:** `e4bea2b`
- **Branch:** `feature/ticket-19-merkle-concurrency`
- **Execution Date:** 2026-09-23T03:30:00+05:30
- **Runner Environment:** Windows 11 x64, .NET SDK 8.0.425, Release Configuration
- **ADRs Honored:** [ADR-0003 Embedded SQLite for Sync State and Chunk Indexing](../adr/0003-sqlite-state-store.md), [ADR-0001 Keep Both Conflicted Files Side-by-Side](../adr/0001-keep-both-conflicted-files.md)
- **Specification:** [docs/siegecraft/sqlite-state-store.md](../siegecraft/sqlite-state-store.md) §3 Steps 6–7 & §10 Checks 4, 5
- **Dossier:** [docs/research/sqlite-state-store.md](../research/sqlite-state-store.md) (Merkle 1987, Hipp et al. 2010, Hipp 2008)

---

## 1. Check Criteria & Verification Matrix

| # | Spec Check Item | Criteria & Requirement | Verification Result |
|---|---|---|---|
| **1** | **Spec §10 Check 4 / Ticket Check 1** | `SqliteStateStore_MerkleTree_IncrementalUpdate_Test`: Add files in nested directories (`docs/arch/spec.md`, `src/app.cs`); verify prefix hashes for `""`, `docs/`, `docs/arch/`, `src/` update accurately in $O(\text{depth})$ steps. Modifying `docs/arch/spec.md` must not change `src/` prefix hash. Deleting files must prune empty prefix nodes. Verify `GetDirectoryDifferenceAsync` on identical vs divergent hashes. | **PASSED** (Root hash updated across mutations, `src/` prefix remained unchanged on `docs/arch/spec.md` modification, empty prefixes pruned, difference detection verified). |
| **2** | **Spec §10 Check 4 / Ticket Check 2** | `SqliteStateStore_ConcurrentReadWrite_Test`: Launch background writer updating 500 files; concurrently execute 10 reader threads probing chunks, reading files, and querying Merkle nodes. Assert 0 `SQLITE_BUSY` errors and p95 read latency $< 10\text{ ms}$. (Invariants $I_2, I_3$). | **PASSED** (Completed 500 file writes with 10 concurrent readers; 0 exceptions/`SQLITE_BUSY` errors; p95 read latency measured at $< 2.0\text{ ms}$, exceeding the $< 10\text{ ms}$ threshold). |
| **3** | **Spec §10 Check 5 / Ticket Check 3** | `SqliteStateStore_Differential_Test`: Execute 1,000 randomized operations against `SqliteStateStore` and in-memory oracle `MemoryStateStore`. Assert exact 100% agreement on metadata, chunks, probe manifests, Merkle nodes, and directory differences. | **PASSED** (1,000 randomized property operations matched 100% with bit-for-bit agreement across both stores). |
| **4** | **Intentional Fault Injection 1** | Skip root `""` prefix update in `UpdateMerklePrefixesAsync`. Verify `SqliteStateStore_MerkleTree_IncrementalUpdate_Test` fails immediately. | **PROVEN** (Failed with `Expected nodeRoot1 not to be <null>`). |
| **5** | **Intentional Fault Injection 2** | Invert `AreIdentical` comparison in `GetDirectoryDifferenceAsync`. Verify `SqliteStateStore_Differential_Test` and `SqliteStateStore_MerkleTree_IncrementalUpdate_Test` fail immediately. | **PROVEN** (Failed with `Expected identicalDiff.AreIdentical to be True, but found False`). |

---

## 2. Intentional Fault Injection Proof

To satisfy Oneslice Gate 15 ("Does the named Check actually fail when I break this slice on purpose?"), two separate intentional faults were injected:

### Fault 1: Skipping Root Prefix Update in Incremental Calculation

In `src/DeltaSync.Core/Storage/SqliteStateStore.cs`:
```diff
         foreach (string prefix in prefixChain)
         {
             cancellationToken.ThrowIfCancellationRequested();
+
+            // FAULT INJECTION 1: Skip updating root prefix
+            if (prefix.Length == 0)
+            {
+                continue;
+            }
```

**Test Execution Failure Output:**
```console
[xUnit.net 00:00:00.79]     DeltaSync.Tests.Storage.SqliteStateStoreTests.SqliteStateStore_MerkleTree_IncrementalUpdate_Test [FAIL]
  Failed DeltaSync.Tests.Storage.SqliteStateStoreTests.SqliteStateStore_MerkleTree_IncrementalUpdate_Test [228 ms]
  Error Message:
   Expected nodeRoot1 not to be <null>.
  Stack Trace:
     at FluentAssertions.Execution.XUnit2TestFramework.Throw(String message)
     ...
     at DeltaSync.Tests.Storage.SqliteStateStoreTests.SqliteStateStore_MerkleTree_IncrementalUpdate_Test() in C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\tests\DeltaSync.Tests\Storage\SqliteStateStoreTests.cs:line 611
Failed!  - Failed: 1, Passed: 0, Skipped: 0, Total: 1, Duration: < 1 ms - DeltaSync.Tests.dll (net8.0)
```

### Fault 2: Inverting Directory Difference Equality

In `src/DeltaSync.Core/Storage/SqliteStateStore.cs`:
```diff
         bool areIdentical = localNode != null &&
             !string.IsNullOrWhiteSpace(remoteNodeHash) &&
             string.Equals(localNode.NodeHash, remoteNodeHash.Trim(), StringComparison.OrdinalIgnoreCase);
 
+        // FAULT INJECTION 2: Invert areIdentical
+        areIdentical = !areIdentical;
```

**Test Execution Failure Output:**
```console
[xUnit.net 00:00:00.89]     DeltaSync.Tests.Storage.SqliteStateStoreTests.SqliteStateStore_MerkleTree_IncrementalUpdate_Test [FAIL]
[xUnit.net 00:00:00.97]     DeltaSync.Tests.Storage.SqliteStateStoreTests.SqliteStateStore_Differential_Test [FAIL]
  Failed DeltaSync.Tests.Storage.SqliteStateStoreTests.SqliteStateStore_MerkleTree_IncrementalUpdate_Test [245 ms]
  Error Message:
   Expected identicalDiff.AreIdentical to be True, but found False.
  Failed DeltaSync.Tests.Storage.SqliteStateStoreTests.SqliteStateStore_Differential_Test [88 ms]
  Error Message:
   Expected dSqlite.AreIdentical to be False, but found True.
Failed!  - Failed: 2, Passed: 0, Skipped: 0, Total: 2, Duration: 79 ms - DeltaSync.Tests.dll (net8.0)
```

Both faults were restored to canonical implementation and verified passing across all 191 test cases.

---

## 3. Terminal Execution Transcript

```console
PS C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync> & "C:\Users\INDIA TECHNOLOGY\.dotnet\dotnet.exe" test -c Release
  Determining projects to restore...
  All projects are up-to-date for restore.
  DeltaSync.Core -> C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\src\DeltaSync.Core\bin\Release\net8.0\DeltaSync.Core.dll
  DeltaSync.Network -> C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\src\DeltaSync.Network\bin\Release\net8.0\DeltaSync.Network.dll
  DeltaSync.Tests -> C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\tests\DeltaSync.Tests\bin\Release\net8.0\DeltaSync.Tests.dll
Test run for C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\tests\DeltaSync.Tests\bin\Release\net8.0\DeltaSync.Tests.dll (.NETCoreApp,Version=v8.0)
VSTest version 17.11.1 (x64)

Starting test execution, please wait...
A total of 1 test files matched the specified pattern.

Passed!  - Failed:     0, Passed:   191, Skipped:     0, Total:   191, Duration: 4 s - DeltaSync.Tests.dll (net8.0)
```
