# Check Dossier: Ticket #38 - MultiProcessCliHarness Supervisor with HTTP Readiness Gating

- **Ticket:** [#38 Implement MultiProcessCliHarness supervisor with HTTP readiness gating](https://github.com/Sehaan-1/DeltaSync/issues/38)
- **Engine Spec:** [docs/siegecraft/multi-peer-cli-demo-engine.md](../siegecraft/multi-peer-cli-demo-engine.md) §3 Steps 1, 3, 5 & §10 Check 2
- **Research Dossier:** [docs/research/multi-peer-cli-demo-harness.md](multi-peer-cli-demo-harness.md) F1, F2, F4
- **ADRs in Force:** [ADR-0001 Keep Both Conflicted Files](../adr/0001-keep-both-conflicted-files.md), [ADR-0004 FastCDC Chunking and Delta Transfer](../adr/0004-fastcdc-chunking-and-delta-transfer.md), [ADR-0005 Observability Terminal and Prometheus](../adr/0005-observability-terminal-and-prometheus.md)
- **Date:** 2026-09-26
- **Branch:** `feature/multiprocess-cli-harness`
- **Target OS:** Windows 11 x64 (.NET SDK 8.0.425)

---

## 1. Summary of Changes

1. **Harness Abstraction & Node Wrapper (`tests/DeltaSync.Tests/Harness/MultiProcessCliHarness.cs`):**
   - Created `CliHarnessOptions` record supporting custom base directory, cluster GUID, startup timeouts (default 3.0s), polling intervals (default 50ms), and per-node extra CLI flags.
   - Created `CliNodeInstance` implementing `IDisposable`, encapsulating child process state, process ID, assigned ports, isolated working directory, asynchronous stdout/stderr streaming queues (preventing OS pipe buffer deadlocks), and tracking process exit states.
   - Implemented `MultiProcessCliHarness` implementing `IAsyncDisposable` with `NodeA`, `NodeB`, `ClusterId`, and `RootDirectory` properties.

2. **Ephemeral Port Allocation (`AllocateDistinctPorts`):**
   - Implemented dynamic allocation of 4 mutually exclusive loopback ports by holding 4 concurrent `TcpListener(IPAddress.Loopback, 0)` instances simultaneously before releasing.
   - Enforces Invariant $I_1$ ($|\{L_A, M_A, L_B, M_B\}| = 4$) and eliminates TCP port collision races or `TIME_WAIT` lockouts (RFC 6335 §6, RFC 9293 §3.5).

3. **Zero-Race Readiness Probing (`WaitForReadinessAsync`):**
   - Implemented HTTP readiness probing polling `GET http://127.0.0.1:<metricsPort>/metrics` with per-request timeouts (200ms) and 50ms polling intervals.
   - Parallelized probing across both child nodes via `Task.WhenAll`.
   - Added immediate premature process exit detection (`node.HasExited` check) to fail fast with exit code, stdout, and stderr without waiting for the full 3.0s timeout.

4. **Leak-Free Process Tree Teardown & Lock Hygiene (`DisposeAsync`):**
   - Issues `Process.Kill(entireProcessTree: true)` to ensure all child processes and subprocesses terminate cleanly.
   - Awaits process exit (`WaitForExitAsync`) with bounded timeout.
   - Implemented 100ms pause and 5-attempt retry loop for recursive temporary directory deletion to accommodate Windows Defender and SQLite WAL unmapping latencies.

5. **Automated Test Suite (`tests/DeltaSync.Tests/Harness/MultiProcessCliHarnessTests.cs`):**
   - Authored 6 automated tests (all 6 passing in 1-2 seconds):
     - `AllocateDistinctPorts_ReturnsFourMutuallyExclusivePorts_Test`: Verifies all 4 allocated ports are distinct and in the dynamic range.
     - `FindCliDll_LocatesCompiledAssembly_Test`: Verifies `DeltaSync.Cli.dll` is discovered across test and build directories.
     - `WaitForReadinessAsync_UnreachablePort_ThrowsTimeoutException_Test`: Verifies bounded timeout behavior on unreachable ports with descriptive error messaging.
     - `MultiProcessCliHarness_Lifecycle_SpawnsTwoNodes_GatesReadiness_AndDisposesCleanly_Test`: Core lifecycle check spawning two CLI processes, verifying HTTP 200 OK on both Prometheus endpoints, asserting zero ANSI escape codes, verifying clean process termination, and asserting zero directory deletion exceptions.
     - `MultiProcessCliHarness_PrematureProcessExit_FailsPromptlyWithDiagnostics_Test`: Verifies fast failure detection when a child process exits before becoming ready.
     - `MultiProcessCliHarness_PreserveDirectories_WhenDeleteOnDisposeFalse_Test`: Verifies directory retention when `DeleteOnDispose` is configured false.

---

## 2. Verification Proof & Terminal Transcripts

### 2.1 MultiProcessCliHarness Test Suite

Command executed:
```powershell
dotnet test --filter MultiProcessCliHarnessTests
```

Terminal output:
```text
Test run for C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\tests\DeltaSync.Tests\bin\Debug\net8.0\DeltaSync.Tests.dll (.NETCoreApp,Version=v8.0)
VSTest version 17.11.1 (x64)

Starting test execution, please wait...
A total of 1 test files matched the specified pattern.

Passed!  - Failed:     0, Passed:     6, Skipped:     0, Total:     6, Duration: 2 s - DeltaSync.Tests.dll (net8.0)
```

### 2.2 Full Solution Test Suite in Release Mode

Command executed:
```powershell
dotnet test -c Release
```

Terminal output:
```text
Test run for C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\tests\DeltaSync.Tests\bin\Release\net8.0\DeltaSync.Tests.dll (.NETCoreApp,Version=v8.0)
VSTest version 17.11.1 (x64)

Starting test execution, please wait...
A total of 1 test files matched the specified pattern.

Passed!  - Failed:     0, Passed:   288, Skipped:     0, Total:   288, Duration: 4 s - DeltaSync.Tests.dll (net8.0)
```

### 2.3 LLM Wiki Knowledge Vault Health & Lint

Command executed:
```powershell
python scripts/wiki_tool.py lint
```

Terminal output:
```text
🔍 RUNNING VAULT HEALTH & INTEGRITY AUDIT...

✅ No broken wikilinks found.
✅ No orphan pages found (all pages are connected).
✅ All pages have valid YAML frontmatter.

=======================================================
🎉 VAULT HEALTH STATUS: PERFECT (100% Coherent)
=======================================================
```

---

## 3. Intentional Fault Injection Proof

To verify that the test suite is sensitive to failures and not rubber-stamping, an intentional defect was injected into `MultiProcessCliHarness.cs`:

- **Fault Injected:** Modified the readiness probe endpoint in `WaitForReadinessAsync` from `/metrics` to `/nonexistent` (simulating HTTP 404 / misconfigured metrics endpoint).
- **Execution:** Ran `dotnet test --filter MultiProcessCliHarness_Lifecycle_SpawnsTwoNodes_GatesReadiness_AndDisposesCleanly_Test`.
- **Observed Result:** The test immediately failed with:
  ```text
  System.TimeoutException : Failed to reach HTTP metrics endpoint on port 50359 within 5.0s. Output: [16:13:24.979] [METRICS] Prometheus scrape server listening on http://127.0.0.1:50359/metrics
  [16:13:25.014] [NET] TCP peer transport listener active on 0.0.0.0:50358
  [16:13:25.015] [INFO] DeltaSync active. Monitoring 'C:\Users\INDIA TECHNOLOGY\AppData\Local\Temp\deltasync-harness-38d1421050854cfeae081e5badf00d8c\NodeA' on port 50358.
  [16:13:25.031] [PEER] Discovered peer node-b-ed61c at 127.0.0.1:50364
  [16:13:25.032] [PEER] Connected to peer node-b-ed61c
  ```
- **Reversion:** Reverted the URL back to `/metrics`. All tests immediately passed.

---

## 4. Contract & Quality Bar Attestations

- [x] **No clear structural regression:** The harness is modular, decoupled from test runners, and reusable across both integration tests and demonstration runners.
- [x] **Zero process leaks:** `Process.Kill(entireProcessTree: true)` terminates child CLI process trees.
- [x] **Zero socket leaks:** Loopback listeners are closed and ephemeral ports are released.
- [x] **Zero Windows SQLite lock contention:** Directories are deleted cleanly with retry loops after process termination.
- [x] **ADRs honored:** ADR-0001 (isolated conflict dirs), ADR-0004 (FastCDC chunk reuse), ADR-0005 (Prometheus HTTP readiness).
- [x] **Scope discipline:** Only Issue #38 was implemented. Issue #39 (E2E sync scenarios) and Issue #41 (Engine 9) were intentionally left untouched for their respective tickets.
