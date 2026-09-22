# Check Verification: Ticket #15 (Peer Registry, Liveness Heartbeat Tracking, and Static Address Backoff)

- **Ticket:** [#15 Implement peer registry, liveness heartbeat tracking, and static address backoff](https://github.com/Sehaan-1/DeltaSync/issues/15)
- **Commit SHA:** `a4f0636`
- **Execution Date:** 2026-09-23T01:28:00+05:30
- **Runner Environment:** Windows 11 x64, .NET SDK 8.0.425, Release Configuration
- **ADR Honored:** [ADR-0002 Hybrid Local Broadcast and Static Peer Discovery](../adr/0002-hybrid-local-and-static-peer-discovery.md)
- **Specification:** [docs/siegecraft/peer-discovery-engine.md](../siegecraft/peer-discovery-engine.md) §3 Steps 3–4 & §10 Check 3
- **Dossier:** [docs/research/peer-discovery-protocol.md](../research/peer-discovery-protocol.md)

---

## 1. Check Criteria & Verification Matrix

| # | Spec Check Item | Criteria & Requirement | Verification Result |
|---|---|---|---|
| **1** | **Spec §10 Check 3.1** | `SpecSection10_Check3_PeerRegistry_LivenessTimeout_TransitionsToStaleAndDead`: Feed simulated beacons to registry, advance virtual time via `TimeProvider`: assert state transitions from `Connected` to `Stale` at 6.0s and `Dead` at 9.0s, emitting `PeerLost` event and evicting dead node. | **PASSED** (Transitions: `Connected` at 0s, unchanged at 5.9s, `Stale` at 6.1s, `Dead` at 9.1s with `PeerLost` event and eviction; Postcondition $P_3$ verified). |
| **2** | **Spec §10 Check 3.2** | `SpecSection10_Check3_StaticPeer_FullJitterBackoff_RespectsBoundsAndVariance`: Consecutive failures across attempts 0..10; retry delay is strictly bounded in $[0, \min(30.0, 1.0 \times 2^a)]$ and exhibits expected variance over 1,000 runs without thundering herd synchronization. | **PASSED** (1,000 samples per attempt; all delays strictly in $[0, \text{cap}]$; mean converges to $\text{cap}/2$ and standard deviation to $\text{cap}/\sqrt{12}$ with full interval spread). |
| **3** | **Spec §10 Check 3.3** | `SpecSection10_Check3_ThreeNode_DiscoveryAndLiveness_VirtualTimeSimulation`: Run three simulated nodes ($A, B, C$) on loopback. Verify nodes $A$ and $B$ discover each other within 3.5s. Stop node $B$'s announcer. Assert node $A$ marks $B$ as `Stale` after 6.0s and `Dead` after 9.0s while node $C$ remains active. | **PASSED** (Node $B$ transitioned to `Stale` at 6.1s silence and evicted as `Dead` at 9.1s silence; Node $C$ remained active with continuous beacons). |
| **4** | **ADR-0002 Hybrid Discovery** | `HybridDiscovery_UnifiesLiveUdpBeacons_And_StaticEndpoints`: Verify `IPeerDiscovery` unifies UDP multicast discovery beacons and configured `--peer <host>:<port>` endpoints into a single peer event stream. | **PASSED** (Static peer available immediately in `ActivePeers`; dynamic peer discovered over loopback UDP socket within 3.5s). |
| **5** | **Dual Indexing & Consistency** | `LookupByPeerId_And_LookupByHash_ReturnConsistentSnapshot`: Verify dual indexing by canonical `PeerId`, SHA-256 `PeerIdHash`, and gRPC service `IPEndPoint`. | **PASSED** (Lookups by string ID and SHA-256 span yield identical snapshots). |
| **6** | **Stale Peer Recovery** | `LivenessTimeout_PeerReceivesBeaconWhileStale_RecoversToDiscovered`: Temporary network blip (peer in `Stale` state) recovers back to `Discovered` immediately when a new beacon arrives. | **PASSED** (State restored and timers refreshed without dropping node). |
| **7** | **Static Endpoint Parsing** | `TryParseEndpoint_ValidFormats_SucceedsWithExactIPAndPort` & malformed rejection: IPv4, IPv6 (`[::1]:port`), hostnames (`localhost:port`), port boundaries ($1 \le P \le 65535$). | **PASSED** (Valid formats parsed cleanly; malformed inputs rejected with descriptive errors and 0 exceptions). |
| **8** | **Intentional Fault Injection** | Disabling dead timeout evaluation in `PeerLivenessTracker.cs` must cause test suite to fail immediately. | **PROVEN** (`SpecSection10_Check3_PeerRegistry_LivenessTimeout_TransitionsToStaleAndDead` failed with `Expected sweep4.TransitionedToDead to be 1, but found 0`). |

---

## 2. Intentional Fault Injection Proof

To satisfy Oneslice Gate 15 ("Does the named Check actually fail when I break this slice on purpose?"), dead timeout evaluation was disabled for stale peers in `PeerLivenessTracker.cs`:

```diff
  case PeerState.Stale:
-     if (elapsed >= _options.DeadTimeout)
+     if (false && elapsed >= _options.DeadTimeout)
      {
          if (_registry.MarkDead(peer.PeerId, _options.EvictOnDead, out _))
          {
              deadCount++;
          }
      }
      break;
```

The test runner immediately reported the failure:

```console
[xUnit.net 00:00:00.57]     DeltaSync.Tests.Network.PeerLivenessTrackerTests.SpecSection10_Check3_PeerRegistry_LivenessTimeout_TransitionsToStaleAndDead [FAIL]
[xUnit.net 00:00:00.58]     DeltaSync.Tests.Network.PeerLivenessTrackerTests.LivenessTimeout_MultiplePeers_StaggeredExpirations [FAIL]
  Failed DeltaSync.Tests.Network.PeerLivenessTrackerTests.SpecSection10_Check3_PeerRegistry_LivenessTimeout_TransitionsToStaleAndDead [83 ms]
  Error Message:
   Expected sweep4.TransitionedToDead to be 1, but found 0 (difference of -1).
  Stack Trace:
     at FluentAssertions.Execution.XUnit2TestFramework.Throw(String message)
     at DeltaSync.Tests.Network.PeerLivenessTrackerTests.SpecSection10_Check3_PeerRegistry_LivenessTimeout_TransitionsToStaleAndDead()
```

The timeout evaluation logic was immediately restored and confirmed passing.

---

## 3. Terminal Transcript

```console
PS C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync> & "C:\Users\INDIA TECHNOLOGY\.dotnet\dotnet.exe" test -c Release --verbosity normal
  Determining projects to restore...
  All projects are up-to-date for restore.
  DeltaSync.Core -> C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\src\DeltaSync.Core\bin\Release\net8.0\DeltaSync.Core.dll
  DeltaSync.Network -> C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\src\DeltaSync.Network\bin\Release\net8.0\DeltaSync.Network.dll
  DeltaSync.Cli -> C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\src\DeltaSync.Cli\bin\Release\net8.0\DeltaSync.Cli.dll
  DeltaSync.Tests -> C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\tests\DeltaSync.Tests\bin\Release\net8.0\DeltaSync.Tests.dll
Test run for C:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\tests\DeltaSync.Tests\bin\Release\net8.0\DeltaSync.Tests.dll (.NETCoreApp,Version=v8.0)
VSTest version 17.11.1 (x64)

Starting test execution, please wait...
A total of 1 test files matched the specified pattern.
[xUnit.net 00:00:00.00] xUnit.net VSTest Adapter v2.8.2+699d445a1a (64-bit .NET 8.0.31)
[xUnit.net 00:00:00.28]   Starting:    DeltaSync.Tests
  Passed DeltaSync.Tests.Network.PeerRegistryTests.RegisterOrUpdateBeacon_NewPeer_InsertsAndFiresDiscoveredEvent [< 1 ms]
  Passed DeltaSync.Tests.Network.PeerRegistryTests.RegisterOrUpdateBeacon_ExistingPeer_RefreshesLastSeenAndEndpoint [< 1 ms]
  Passed DeltaSync.Tests.Network.PeerRegistryTests.RegisterOrUpdateStatic_InsertsStaticPeerAndFiresDiscoveredEvent [< 1 ms]
  Passed DeltaSync.Tests.Network.PeerRegistryTests.LookupByPeerId_And_LookupByHash_ReturnConsistentSnapshot [< 1 ms]
  Passed DeltaSync.Tests.Network.PeerRegistryTests.MarkConnected_TransitionsToConnected_ResetsConsecutiveFailures [< 1 ms]
  Passed DeltaSync.Tests.Network.PeerRegistryTests.MarkStale_TransitionsToStale_FiresStaleEvent [< 1 ms]
  Passed DeltaSync.Tests.Network.PeerRegistryTests.MarkDead_TransitionsToDead_EvictsAndFiresLostEvent [< 1 ms]
  Passed DeltaSync.Tests.Network.PeerRegistryTests.StalePeer_ReceivesBeacon_TransitionsBackToDiscovered [< 1 ms]
  Passed DeltaSync.Tests.Network.PeerLivenessTrackerTests.SpecSection10_Check3_PeerRegistry_LivenessTimeout_TransitionsToStaleAndDead [2 ms]
  Passed DeltaSync.Tests.Network.PeerLivenessTrackerTests.LivenessTimeout_PeerReceivesBeaconWhileStale_RecoversToDiscovered [< 1 ms]
  Passed DeltaSync.Tests.Network.PeerLivenessTrackerTests.LivenessTimeout_MultiplePeers_StaggeredExpirations [< 1 ms]
  Passed DeltaSync.Tests.Network.StaticPeerProviderTests.SpecSection10_Check3_StaticPeer_FullJitterBackoff_RespectsBoundsAndVariance [86 ms]
  Passed DeltaSync.Tests.Network.StaticPeerProviderTests.StaticPeerProvider_RegistryIntegration_UpdatesConsecutiveFailuresAndBackoff [1 ms]
  Passed DeltaSync.Tests.Network.StaticPeerProviderTests.TryParseEndpoint_ValidFormats_SucceedsWithExactIPAndPort [< 1 ms]
  Passed DeltaSync.Tests.Network.StaticPeerProviderTests.TryParseEndpoint_MalformedInputs_ReturnsFalseWithDescriptiveError [< 1 ms]
  Passed DeltaSync.Tests.Network.PeerDiscoveryEngineIntegrationTests.SpecSection10_Check3_ThreeNode_DiscoveryAndLiveness_VirtualTimeSimulation [2 ms]
  Passed DeltaSync.Tests.Network.PeerDiscoveryEngineIntegrationTests.HybridDiscovery_UnifiesLiveUdpBeacons_And_StaticEndpoints [117 ms]
  ...
Test Run Successful.
Total tests: 139
     Passed: 139
 Total time: 3.2384 Seconds

Build succeeded.
    0 Warning(s)
    0 Error(s)
```
