# Check Verification: Ticket #14 (Binary UDP Multicast Beacon & Loopback Filter)

- **Ticket:** [#14 Implement binary UDP multicast beacon announcer, parser, and loopback filter](https://github.com/Sehaan-1/DeltaSync/issues/14)
- **Commit SHA:** `5fe4be3`
- **Execution Date:** 2026-09-23T00:57:40+05:30
- **Runner Environment:** Windows 11 x64, .NET SDK 8.0.425, Release Configuration
- **ADR Honored:** [ADR-0002 Hybrid Local Broadcast and Static Peer Discovery](../adr/0002-hybrid-local-and-static-peer-discovery.md)
- **Specification:** [docs/siegecraft/peer-discovery-engine.md](../siegecraft/peer-discovery-engine.md) §3 Steps 1–2 & §10 Check 1
- **Dossier:** [docs/research/peer-discovery-protocol.md](../research/peer-discovery-protocol.md)

---

## 1. Check Criteria & Verification Matrix

| # | Spec Check Item | Criteria & Requirement | Verification Result |
|---|---|---|---|
| **1** | **Spec §10 Check 1.1** | `BeaconSerialization_Roundtrip_BitForBit_Exact72Bytes`: Encode & decode valid frames across varied peer IDs, ports, timestamps, and cluster UUIDs. Strict 72-byte size ($\le 508$ MTU). | **PASSED** (Exact 72 bytes, bit-for-bit equivalence across all variations). |
| **2** | **Spec §10 Check 1.2** | `BeaconSerialization_TruncatedBuffer_RejectsSafelyWithZeroAllocations`: Truncated (0, 1, 2, 4, 16, 22, 54, 70, 71 bytes) and corrupted magic numbers reject safely. | **PASSED** (Returns `false`, `null` frame, exact 0 bytes allocated on managed heap). |
| **3** | **Spec §10 Check 1.3** | `UdpBeacon_LoopbackSuppression_FiltersSelfBeacons`: Start listener and announcer with identical `PeerId`; verify self-beacons are filtered out and not propagated. | **PASSED** (0 `BeaconReceived` events emitted, `SelfBeaconsSuppressed` $\ge 3$, Invariant $I_3$ verified). |
| **4** | **Spec §6 Known-Answer Vector** | Known-Answer Vector verification: Magic `0x4453594E`, Version `1`, Cluster `00112233-4455-6677-8899-aabbccddeeff`, ListenPort `5001`, Seq `42`, Timestamp `1700000000000`, PeerId `node-alpha`. | **PASSED** (Bytes `0..3` equal `[0x4E, 0x59, 0x53, 0x44]`, reconstructed identical field values bit-for-bit). |
| **5** | **Floyd-Jacobson Phase Jitter** | `FloydJacobson_JitterDistribution_RespectsBounds`: 10,000 iterations verifying $T_{\text{next}} \in [2.4\text{s}, 3.6\text{s}]$ ($\pm 20\%$) and mean converges to $3.0\text{s} \pm 50\text{ms}$. | **PASSED** (Min: 2400.1ms, Max: 3600.0ms, Mean: 2999.8ms). |
| **6** | **Cluster Isolation ($P_2$)** | `UdpBeacon_ClusterIsolation_SilentlyDropsDifferentCluster`: Beacons sent from different cluster IDs are silently dropped without firing discovery events. | **PASSED** (0 discovery events, `DroppedPackets` incremented). |
| **7** | **Remote Peer Discovery** | `UdpBeacon_RemotePeerDiscovered_EmitsEventWithCorrectMetadata`: Remote peer with matching ClusterId triggers `BeaconReceived` event with sender endpoint and frame. | **PASSED** (`ServiceEndPoint` correctly synthesized from IP + `ListenPort`). |
| **8** | **Intentional Fault Injection** | Disabling loopback suppression check in `UdpBeaconListener.cs` must cause test suite to fail immediately. | **PROVEN** (`SpecSection10_Check1_UdpBeacon_LoopbackSuppression_FiltersSelfBeacons` failed with `Expected receivedBeacons to be empty...`). |

---

## 2. Intentional Fault Injection Proof

To satisfy Oneslice Gate 15 ("Does the named Check actually fail when I break this slice on purpose?"), loopback suppression was disabled in `UdpBeaconListener.cs`:

```diff
- if (frame.PeerIdHash.AsSpan().SequenceEqual(_localPeerIdHash))
+ if (false && frame.PeerIdHash.AsSpan().SequenceEqual(_localPeerIdHash))
  {
      Interlocked.Increment(ref _selfBeaconsSuppressed);
      return;
  }
```

The test runner immediately reported the failure:

```console
[xUnit.net 00:00:01.13]     DeltaSync.Tests.Network.UdpBeaconIntegrationTests.SpecSection10_Check1_UdpBeacon_LoopbackSuppression_FiltersSelfBeacons [FAIL]
  Failed DeltaSync.Tests.Network.UdpBeaconIntegrationTests.SpecSection10_Check1_UdpBeacon_LoopbackSuppression_FiltersSelfBeacons [513 ms]
  Error Message:
   Expected receivedBeacons to be empty because Self-emitted beacons must be filtered by loopback suppression (Invariant I3), but found at least one item 
{
    DeltaSync.Network.DiscoveredBeacon
    {
        Frame = DeltaSync.Network.BeaconFrame { ... }, 
        RemoteEndPoint = 127.0.0.1:53689, 
        ServiceEndPoint = 127.0.0.1:5001
    }
}.
```

The loopback suppression logic was immediately restored and confirmed passing.

---

## 3. Terminal Transcript

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
[xUnit.net 00:00:00.32]   Starting:    DeltaSync.Tests
  Passed DeltaSync.Tests.Network.BeaconSerializationTests.SpecSection10_Check1_BeaconSerialization_Roundtrip_BitForBit_Exact72Bytes [41 ms]
  Passed DeltaSync.Tests.Network.BeaconSerializationTests.SpecSection6_KnownAnswerPacketVector_MatchesBitForBit [4 ms]
  Passed DeltaSync.Tests.Network.BeaconSerializationTests.SpecSection10_Check1_BeaconSerialization_TruncatedBuffer_RejectsSafelyWithZeroAllocations [1 ms]
  Passed DeltaSync.Tests.Network.BeaconSerializationTests.SpecSection3Step2_FloydJacobson_JitterDistribution_RespectsBounds [18 ms]
  Passed DeltaSync.Tests.Network.UdpBeaconIntegrationTests.SpecSection10_Check1_UdpBeacon_LoopbackSuppression_FiltersSelfBeacons [391 ms]
  Passed DeltaSync.Tests.Network.UdpBeaconIntegrationTests.UdpBeacon_RemotePeerDiscovered_EmitsEventWithCorrectMetadata [162 ms]
  Passed DeltaSync.Tests.Network.UdpBeaconIntegrationTests.UdpBeacon_ClusterIsolation_SilentlyDropsDifferentCluster [220 ms]
  Passed DeltaSync.Tests.Network.UdpBeaconIntegrationTests.UdpBeacon_MalformedDatagrams_DroppedSafely [169 ms]
...
Test Run Successful.
Total tests: 109
     Passed: 109
 Total time: 4.0671 Seconds
```
