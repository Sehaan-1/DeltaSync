# Check Artifact: Ticket #21 — Peer Merkle Difference Negotiation & Chunk Wire Protocol

- **Ticket:** [Issue #21: Implement peer Merkle difference negotiation and chunk transfer wire protocol](https://github.com/Sehaan-1/DeltaSync/issues/21)
- **Slice:** Slice 2 (Engine 6 Slice 2: Peer Merkle Difference Negotiation & Chunk Wire Protocol)
- **Engine Spec:** [docs/siegecraft/sync-orchestration-engine.md](../siegecraft/sync-orchestration-engine.md) §2.1, §2.2, §3 Steps 2–3
- **ADRs Honored:** [ADR-0002](file:///c:/Users/INDIA%20TECHNOLOGY/Documents/DeltaSync/docs/adr/0002-hybrid-local-and-static-peer-discovery.md), [ADR-0004](file:///c:/Users/INDIA%20TECHNOLOGY/Documents/DeltaSync/docs/adr/0004-fastcdc-chunking.md)
- **Base Commit:** `e4fe91b`
- **Date:** 2026-09-23T16:28:00+05:30

---

## 1. What Was Implemented

1. **Core Port Realignment ([`src/DeltaSync.Core/Network/IPeerTransportChannel.cs`](file:///c:/Users/INDIA%20TECHNOLOGY/Documents/DeltaSync/src/DeltaSync.Core/Network/IPeerTransportChannel.cs)):**
   - Relocated `IPeerTransportChannel` from `DeltaSync.Network` into `DeltaSync.Core/Network/` as an external port owned by Core ([`docs/architecture/deltasync.md`](file:///c:/Users/INDIA%20TECHNOLOGY/Documents/DeltaSync/docs/architecture/deltasync.md) § Ports the Core Owns), keeping namespace `DeltaSync.Network` for 100% source and binary compatibility across existing tests and network components.

2. **Wire Framing & Messages ([`src/DeltaSync.Core/Sync/Wire/`](file:///c:/Users/INDIA%20TECHNOLOGY/Documents/DeltaSync/src/DeltaSync.Core/Sync/Wire/)):**
   - **`SyncMessageType` ([`src/DeltaSync.Core/Sync/Wire/SyncMessageType.cs`](file:///c:/Users/INDIA%20TECHNOLOGY/Documents/DeltaSync/src/DeltaSync.Core/Sync/Wire/SyncMessageType.cs)):** 1-byte framing discriminator enum (`MerkleRootProbe = 0x01`, `MerkleRootResponse = 0x02`, `PrefixDiffRequest = 0x03`, `PrefixDiffResponse = 0x04`, `FileManifestQuery = 0x05`, `FileManifestResponse = 0x06`, `ChunkFetchRequest = 0x07`, `ChunkPayloadResponse = 0x08`, `SyncCompletedNotice = 0x09`).
   - **`SyncWireMessages` ([`src/DeltaSync.Core/Sync/Wire/SyncWireMessages.cs`](file:///c:/Users/INDIA%20TECHNOLOGY/Documents/DeltaSync/src/DeltaSync.Core/Sync/Wire/SyncWireMessages.cs)):** Immutable wire records including `MerkleRootProbe`, `MerkleRootResponse`, `PrefixDiffRequest`, `PrefixDiffResponse`, `FileManifestQuery`, `FileManifestResponse`, `ChunkFetchRequest`, `ChunkPayloadResponse`, and `SyncCompletedNotice`, with lightweight wire DTOs (`WireFileRecord`, `WireMerkleNodeRecord`, `WireChunkRecord`).
   - **`SyncWireFrameSerializer` ([`src/DeltaSync.Core/Sync/Wire/SyncWireFrameSerializer.cs`](file:///c:/Users/INDIA%20TECHNOLOGY/Documents/DeltaSync/src/DeltaSync.Core/Sync/Wire/SyncWireFrameSerializer.cs)):** High-performance framing serializer with 8-byte little-endian correlation sequence ID (`ulong CorrelationId`) preceding payload. Employs zero-copy binary streaming for chunk payloads (32-byte binary SHA-256 hash + raw chunk bytes, eliminating 33% base64 encoding bloat), compact UTF-8 JSON for metadata frames, maximum frame safety limits (4MB), and strict path traversal validation.

3. **Wire Protocol Port & Implementation ([`src/DeltaSync.Core/Sync/`](file:///c:/Users/INDIA%20TECHNOLOGY/Documents/DeltaSync/src/DeltaSync.Core/Sync/)):**
   - **`ISyncWireProtocol` ([`src/DeltaSync.Core/Sync/ISyncWireProtocol.cs`](file:///c:/Users/INDIA%20TECHNOLOGY/Documents/DeltaSync/src/DeltaSync.Core/Sync/ISyncWireProtocol.cs)):** Contract defining client synchronization operations (`ProbeRootEqualityAsync`, `ReconcilePrefixAsync`, `FetchManifestAsync`, `FetchChunkAsync`, `FetchChunksPipelinedAsync`, `FetchAndReconstructFileAsync`) and duplex channel attachment (`AttachChannel`).
   - **`SyncWireProtocol` ([`src/DeltaSync.Core/Sync/SyncWireProtocol.cs`](file:///c:/Users/INDIA%20TECHNOLOGY/Documents/DeltaSync/src/DeltaSync.Core/Sync/SyncWireProtocol.cs)):** Complete asynchronous protocol implementation. Runs full-duplex background receive loops correlating response frames to pending `TaskCompletionSource`s via `CorrelationId`, handles incoming remote requests against `ISqliteStateStore` and `ILocalChunkProvider`, performs $O(\log N)$ hierarchical BFS prefix difference traversal stopping early at identical subtrees, and pipelines chunk requests using a sliding window ($W = 16$).
   - **`ISyncMetricsSink` ([`src/DeltaSync.Core/Sync/ISyncMetricsSink.cs`](file:///c:/Users/INDIA%20TECHNOLOGY/Documents/DeltaSync/src/DeltaSync.Core/Sync/ISyncMetricsSink.cs)):** Telemetry port for recording transfer bytes, chunk deduplication savings, conflicts, and cycle durations.

4. **Test Suites ([`tests/DeltaSync.Tests/Sync/`](file:///c:/Users/INDIA%20TECHNOLOGY/Documents/DeltaSync/tests/DeltaSync.Tests/Sync/)):**
   - **`SyncWireFrameSerializerTests`:** 8 unit tests covering all 9 frame types, correlation ID preservation, binary chunk payload integrity, truncated frame handling, and path traversal rejection.
   - **`SyncWireProtocolTests`:** 8 unit tests covering root equality probing, prefix difference pruning, manifest exchange, chunk retrieval, sliding-window pipelining ($W = 16$), and file reconstruction.
   - **`SyncWireProtocolIntegrationTests`:** The Slice Check verifying 1,000 files root matching in 2 messages, 10MB file 2-line edit transferring $<200\text{ KB}$ with $>98\%$ savings, and Gate 15 fault injection.

---

## 2. The Check: Prefix Divergence & Chunk Transfer Verification

**Check Definition (Ticket #21 Check):**
Automated integration test `SyncWireProtocol_PrefixDivergenceAndChunkTransfer_Test` over `InMemoryTransportChannel`:
1. Reconcile two peers with 1,000 identical files: assert exactly 2 messages exchanged (`MerkleRootProbe`/`Response`) and 0 chunk payload bytes transferred.
2. Introduce a 2-line edit in a 10MB file on Peer A: assert traversal identifies only the modified file, queries manifest, and transfers $< 200\text{ KB}$ of payload chunks ($>98\%$ bandwidth savings).

### Test Execution Transcript:
```powershell
& "C:\Users\INDIA TECHNOLOGY\.dotnet\dotnet.exe" test --filter "FullyQualifiedName~SyncWireProtocolIntegrationTests" --logger "console;verbosity=detailed"
```
```text
Test run for C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\tests\DeltaSync.Tests\bin\Debug\net8.0\DeltaSync.Tests.dll (.NETCoreApp,Version=v8.0)
VSTest version 17.11.1 (x64)

Starting test execution, please wait...
A total of 1 test files matched the specified pattern.
C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\tests\DeltaSync.Tests\bin\Debug\net8.0\DeltaSync.Tests.dll
[xUnit.net 00:00:00.00] xUnit.net VSTest Adapter v2.8.2+699d445a1a (64-bit .NET 8.0.31)
[xUnit.net 00:00:00.07]   Discovering: DeltaSync.Tests
[xUnit.net 00:00:00.20]   Discovered:  DeltaSync.Tests
[xUnit.net 00:00:00.21]   Starting:    DeltaSync.Tests
[xUnit.net 00:00:01.26]   Finished:    DeltaSync.Tests
  Passed DeltaSync.Tests.Sync.SyncWireProtocolIntegrationTests.Gate15_FaultInjection_CorruptedChunkPayload_ThrowsChunkIntegrityException [59 ms]
  Passed DeltaSync.Tests.Sync.SyncWireProtocolIntegrationTests.SyncWireProtocol_PrefixDivergenceAndChunkTransfer_Test [942 ms]
  Standard Output Messages:
 === PART 1: Benchmarking 1,000 identical files root probe ===
 Part 1 Passed: Exactly 2 messages exchanged over wire. 0 chunk bytes transferred.
 === PART 2: Benchmarking 10MB file with 2-line delta edit ===
 10MB Base File Chunks: 134
 10MB Modified File Chunks: 134
 Total Chunks in Remote File: 134
 Chunks already present on Peer B: 133
 Missing chunks requiring wire transfer: 1
 Total Chunk Payload Bytes Transferred: 107,138 bytes
 Bandwidth Savings: 98.98%

Test Run Successful.
Total tests: 2
     Passed: 2
 Total time: 1.8433 Seconds
```

---

## 3. Gate 15: Intentional Mechanical Fault Injection

To satisfy **Oneslice Hard Gate 15**, the sensitivity and validity of the verification check was tested by introducing an intentional mechanical failure into `SyncWireProtocol.cs`:

### Injected Fault:
Disabled cryptographic SHA-256 chunk hash verification in `SyncWireProtocol.FetchChunkAsync`:
```csharp
// Intentional fault injection
if (false && !string.Equals(computedHex, chunkHash, StringComparison.OrdinalIgnoreCase))
{
    throw new ChunkIntegrityException(0, chunkHash, computedHex);
}
```

### Result: Immediate Test Failure
```text
[xUnit.net 00:00:00.54]     DeltaSync.Tests.Sync.SyncWireProtocolIntegrationTests.Gate15_FaultInjection_CorruptedChunkPayload_ThrowsChunkIntegrityException [FAIL]
  Failed DeltaSync.Tests.Sync.SyncWireProtocolIntegrationTests.Gate15_FaultInjection_CorruptedChunkPayload_ThrowsChunkIntegrityException [124 ms]
  Error Message:
   Expected a <DeltaSync.Core.Chunking.ChunkIntegrityException> to be thrown, but no exception was thrown.
  Stack Trace:
     at FluentAssertions.Execution.XUnit2TestFramework.Throw(String message)
     ...
     at DeltaSync.Tests.Sync.SyncWireProtocolIntegrationTests.Gate15_FaultInjection_CorruptedChunkPayload_ThrowsChunkIntegrityException()
```
The test failed immediately, confirming that the check accurately detects corrupted chunk payloads.

### Restoration:
The check was restored to active verification, and the suite returned to 100% green:
```text
Passed!  - Failed:     0, Passed:     1, Skipped:     0, Total:     1, Duration: < 1 ms
```

---

## 4. Full Release Test Suite Baseline

```powershell
& "C:\Users\INDIA TECHNOLOGY\.dotnet\dotnet.exe" test -c Release
```
```text
Passed!  - Failed:     0, Passed:   226, Skipped:     0, Total:   226, Duration: 4 s - DeltaSync.Tests.dll (net8.0)
```
- Total Tests: 226 passed, 0 failed, 0 skipped.
- Compiler Warnings: 0.
- Compiler Errors: 0.
