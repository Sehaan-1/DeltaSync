# Check Dossier: Ticket #35 - TcpPeerListener & TcpPeerDialer

- **Ticket:** [#35 Implement TCP peer listener, dialer, and clean BGP collision teardown](https://github.com/Sehaan-1/DeltaSync/issues/35)
- **Engine Spec:** [docs/siegecraft/tcp-tls-transport-engine.md](../siegecraft/tcp-tls-transport-engine.md) §3 Steps 3–5
- **Research Dossier:** [docs/research/tcp-tls-transport-protocol.md](tcp-tls-transport-protocol.md)
- **ADR in Force:** [docs/adr/0002-hybrid-local-and-static-peer-discovery.md](../adr/0002-hybrid-local-and-static-peer-discovery.md)
- **Date:** 2026-09-26
- **Branch:** `feature/tcp-peer-listener-dialer`
- **Target OS:** Windows 11 x64 (.NET SDK 8.0.425)

---

## 1. Summary of Changes

1. **TcpPeerListener (`src/DeltaSync.Network/TcpPeerListener.cs`):**
   - Binds local TCP endpoint with support for both explicit ports and OS-allocated dynamic ports (`port 0`), updating `LocalEndPoint` upon start.
   - Executes an asynchronous accept loop wrapping incoming connections with `NoDelay = true` and instantiating inbound `TcpTransportChannel` instances.
   - Forwards inbound channels asynchronously to `IPeerConnectionCoordinator.AcceptConnectionAsync` with comprehensive exception isolation so individual handshake failures never crash the accept loop.
   - Supports graceful asynchronous shutdown via `StopAsync()`, `Dispose()`, and `DisposeAsync()`.

2. **TcpPeerDialer (`src/DeltaSync.Network/TcpPeerDialer.cs`):**
   - Implements `PeerChannelDialer` delegate pattern with implicit type conversion.
   - Connects to remote `IPEndPoint` with strict timeout (default 5.0 seconds) and `NoDelay = true`.
   - Resolves peer endpoints from `PeerRegistry` if not explicitly specified.
   - Returns outbound `TcpTransportChannel` configured with `isInbound: false` to `PeerConnectionCoordinator.ConnectAsync`.

3. **BGP Collision Teardown & Inbound Identity Resolution (`src/DeltaSync.Network/PeerConnectionCoordinator.cs`):**
   - Populates `RemotePeerId` on accepted inbound transport channels upon completing introductory handshake negotiation so peer identity is canonically bound.
   - Added `PruneDisconnected()` to automatically evict remotely dropped or closed sockets from `_activeConnections` and fire `ConnectionClosed` when `ActiveConnections` or `TryGetConnection` is accessed.

4. **Integration Test Suite (`tests/DeltaSync.Tests/Network/TcpPeerConnectionCoordinatorIntegrationTests.cs`):**
   - Authored 5 comprehensive integration tests verifying physical TCP loopback socket interactions:
     1. `SimultaneousCrossDial_OverPhysicalLoopbackSockets_ConvergesToSingleChannel`: Verifies RFC 4271 §6.8 BGP-4 collision convergence, preserving higher peer outbound dial, cleanly aborting lower peer dial with `CollisionRejected` and zero socket leaks, and exchanging 16 KB and 32 KB ping-pong payloads over the surviving physical connection.
     2. `GracefulDisconnect_CleansUpPeerRegistry`: Closes channel from one side; verifies remote node detects EOF (`ReadOnlyMemory<byte>.Empty`), transitions to disconnected, and evicts peer from active connections.
     3. `ClusterIsolation_OverPhysicalSockets_RejectedCleanly`: Validates cross-cluster connections are rejected with `ClusterMismatch` without leakage.
     4. `DialAsync_NullEndpointAndNoRegistry_ThrowsArgumentException`: Validates parameter edge boundaries.
     5. `DialAsync_NonRoutableEndpointWithShortTimeout_ThrowsTimeoutException`: Validates timeout handling on unresponsive targets.

---

## 2. Verification Proof & Terminal Transcript

### 2.1 Automated Integration Tests (`TcpPeerConnectionCoordinatorIntegrationTests`)

Command executed:
```powershell
dotnet test --filter TcpPeerConnectionCoordinatorIntegrationTests
```

Terminal output:
```text
Test run for C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\tests\DeltaSync.Tests\bin\Debug\net8.0\DeltaSync.Tests.dll (.NETCoreApp,Version=v8.0)
VSTest version 17.11.1 (x64)

Starting test execution, please wait...
A total of 1 test files matched the specified pattern.

Passed!  - Failed:     0, Passed:     5, Skipped:     0, Total:     5, Duration: 404 ms - DeltaSync.Tests.dll (net8.0)
```

### 2.2 Full Regression Test Suite (Release Configuration)

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

Passed!  - Failed:     0, Passed:   273, Skipped:     0, Total:   273, Duration: 6 s - DeltaSync.Tests.dll (net8.0)
```

---

## 3. Intentional Fault Injection Proof

To prove that the test suite detects regressions:
- **Fault Injected:** Inverted BGP tie-break resolution check in `PeerConnectionCoordinator.cs`:
  ```csharp
  // Inverted check:
  if (decision == CollisionDecision.YieldToInbound)
  ```
- **Execution Result:**
  ```text
  [xUnit.net 00:00:01.00] DeltaSync.Tests.Network.TcpPeerConnectionCoordinatorIntegrationTests.SimultaneousCrossDial_OverPhysicalLoopbackSockets_ConvergesToSingleChannel [FAIL]
    Failed DeltaSync.Tests.Network.TcpPeerConnectionCoordinatorIntegrationTests.SimultaneousCrossDial_OverPhysicalLoopbackSockets_ConvergesToSingleChannel [481 ms]
    Error Message:
     Expected activeAtB!.IsInbound to be False because Node Beta has higher PeerId and must preserve outbound dial, but found True.
  ```
- **Outcome:** The integration test suite caught the inversion immediately. Restoring correct code brought the test run back to 5/5 passed.

---

## 4. Vault Health Verification

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
