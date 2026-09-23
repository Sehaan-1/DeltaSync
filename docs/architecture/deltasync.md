# Architecture: DeltaSync Distributed Peer-to-Peer File Synchronization System

## Provenance
- Handoff source: [docs/cuecards/handoff-deltasync.md](../cuecards/handoff-deltasync.md)
- Live Board: [DeltaSync Board Issue #1](https://github.com/Sehaan-1/DeltaSync/issues/1)
- ADRs in Force:
  - [ADR-0001 Keep Both Conflicted Files Side-by-Side](../adr/0001-keep-both-conflicted-files.md)
  - [ADR-0002 Hybrid Local Broadcast and Static Peer Discovery](../adr/0002-hybrid-local-and-static-peer-discovery.md)
  - [ADR-0003 Embedded SQLite for Sync State and Chunk Indexing](../adr/0003-sqlite-state-store.md)
  - [ADR-0004 FastCDC Content-Defined Chunking for Delta Transfer](../adr/0004-fastcdc-chunking.md)
  - [ADR-0005 Terminal Status Dashboard with Prometheus Metrics Endpoint](../adr/0005-observability-terminal-and-prometheus.md)
- Architectural Style: Hexagonal / Ports & Adapters with Domain-Driven Tactical Aggregates.

---

## Destination and Forcing Constraints

1. **Decentralized LAN & WAN Convergence:** Machines discover one another via UDP multicast beacons on local subnets or static addresses over WAN/VPN without central cloud brokers or relays.
2. **Bandwidth Minimization via FastCDC:** Content-defined chunking ($16\text{ KB}$ min, $64\text{ KB}$ target, $256\text{ KB}$ max) with Gear rolling hashes prevents full-file retransfers on localized byte insertions/deletions.
3. **Causal Ordering & No Data Loss:** Concurrent disconnected modifications are detected via immutable vector clocks ($V_A \parallel V_B$) and resolved non-destructively by preserving both variants side-by-side (ADR-0001).
4. **Crash Safety & Acid Transactions:** All chunk staging writes into temporary files before SHA-256 verification and atomic rename. Metadata state store runs in WAL mode with transactional atomicity.
5. **Strict Inward Dependency Rule:** `DeltaSync.Core` is purely policy and domain models. Adapters (Network, SQLite, FileSystem, CLI) depend inward on Core abstractions.

---

## Context Map

```
┌────────────────────────────────────────────────────────────────────────┐
│                              DeltaSync.Cli                             │
│                     (Host, CLI Commands, Dashboard)                    │
└──────────────────┬─────────────────────────────────┬───────────────────┘
                   │                                 │
                   ▼                                 ▼
┌──────────────────────────────────────┐  ┌──────────────────────────────┐
│          DeltaSync.Network           │  │     DeltaSync.Core.Storage   │
│ (UDP Beacons, TCP/P2P Wire Channels) │  │  (SQLite WAL, Migrations)    │
└──────────────────┬───────────────────┘  └──────────────┬───────────────┘
                   │                                     │
                   ▼                                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│                            DeltaSync.Core                              │
│  ┌───────────────────────┐ ┌──────────────────────┐ ┌───────────────┐  │
│  │       Causality       │ │       Chunking       │ │     Sync      │  │
│  │ (Vector Clock, Merge) │ │ (FastCDC, Manifests) │ │ (Orchestrator)│  │
│  └───────────────────────┘ └──────────────────────┘ └───────────────┘  │
└────────────────────────────────────────────────────────────────────────┘
```

### Context: Causality (`DeltaSync.Core.Causality`)
- **Role:** Pure domain value objects and algebraic evaluation for causal history.
- **Key Types:** `VectorClock`, `CausalRelation`, `ConflictResolver`, `ConflictResolutionResult`, `ConflictedPathHelper`.
- **Relationship:** Upstream supplier to Sync Orchestration and Storage. Completely free of IO, network, or database dependencies.

### Context: Chunking (`DeltaSync.Core.Chunking`)
- **Role:** Content-defined boundary cutting, rolling hash evaluation, chunk fingerprinting, and delta file reconstruction.
- **Key Types:** `FastCdcChunker`, `FastCdcConfig`, `ChunkDescriptor`, `FileManifest`, `StreamingFastCdcReader`, `DeltaReconstructor`.
- **Relationship:** Upstream supplier to Sync Orchestration and Storage. Uses pure memory buffers (`ReadOnlyMemory<byte>`, `ReadOnlySpan<byte>`).

### Context: Storage & Persistence (`DeltaSync.Core.Storage`)
- **Role:** Atomic persistent state indexing, file metadata tracking, inverted chunk index, ref-count garbage collection, and incremental Merkle prefix tree digest computation.
- **Key Types:** `ISqliteStateStore`, `SqliteStateStore`, `FileMetadata`, `ChunkLocation`, `MerkleNode`, `DirectoryDifference`, `MerkleTreeHelper`, `MemoryStateStore`.
- **Relationship:** Implements storage ports; consumes Causality and Chunking types.

### Context: Network & Transport (`DeltaSync.Network`)
- **Role:** Peer discovery via UDP multicast beacon broadcast, static address retries, peer registry liveness tracking, connection coordination with BGP collision tie-breaking, and duplex transport channels.
- **Key Types:** `UdpBeaconAnnouncer`, `UdpBeaconListener`, `PeerRegistry`, `PeerConnectionCoordinator`, `IPeerTransportChannel`, `InMemoryTransportChannel`, `BgpCollisionTieBreaker`.
- **Relationship:** Adapter depending inward on `DeltaSync.Core`. Implements peer discovery and communication channels.

### Context: Sync Orchestration (`DeltaSync.Core.Sync`)
- **Role:** Binds local filesystem events, Merkle tree prefix queries, peer wire negotiation, missing chunk transfer, and atomic file commit into an automated convergence loop.
- **Key Types:** `ISyncWireProtocol`, `SyncOrchestrator`, `IFileWatcherService`, `SyncSession`.
- **Relationship:** High-level policy orchestrating all engines.

### Context: Observability & CLI (`DeltaSync.Cli`)
- **Role:** Application composition root (`Program.cs`), Spectre.Console terminal dashboard rendering, and Prometheus telemetry endpoint exporter.
- **Relationship:** Outermost client/driver. Humble object wiring dependencies.

---

## Distillation

The core domain model represents a decentralized, eventually-consistent file synchronizer governed by mathematical invariants:
1. **Content Identity:** A chunk's identity is strictly its cryptographic hash $H(C)$. The same chunk bytes anywhere in the directory tree or history have identical identity and are stored once.
2. **Causal Precedence:** State transitions for a file $F$ are parameterized by vector clock $V_F$. If $V_F^A > V_F^B$, $A$ causally succeeds $B$. If $V_F^A \parallel V_F^B$, concurrent modification occurred.
3. **Prefix Partitioning:** A directory tree is partitioned into recursive prefix digests $H(P)$. Two peers with identical root prefix $H("") = H'("")$ are provably in identical sync states without comparing individual files.

---

## Aggregate Boundaries

### Aggregate 1: `FileState` (Root: `FileMetadata`)
- **Boundary:** Encapsulates path, size, modification timestamp, content hash, vector clock, and ordered chunk references.
- **Invariant:** A file cannot be committed without a valid vector clock, verified content hash, and consistent chunk list matching the total file size. Updating a file atomically re-indexes chunk reference counts in the inverted index.

### Aggregate 2: `MerkleDirectory` (Root: `MerkleNode`)
- **Boundary:** Encapsulates directory prefix, recursive digest, child count, and update timestamp.
- **Invariant:** $H(D) = \text{SHA256}(\sum \text{name}(f) \circ R_f \parallel \sum \text{name}(S) \circ H(S))$. Any file mutation under prefix $P$ triggers bottom-up recalculation of all ancestor prefixes up to root `""` within the same atomic database transaction.

### Aggregate 3: `PeerSession` (Root: `PeerRecord`)
- **Boundary:** Encapsulates peer ID, endpoint addresses, liveness state (`Active`, `Stale`, `Dead`), and active transport channel.
- **Invariant:** Exactly one transport channel exists per peer pair. Simultaneous connection attempts are resolved deterministically by `BgpCollisionTieBreaker`.

---

## Dependency Rule

Dependencies strictly point inward toward domain policy:
```
[DeltaSync.Cli]  ──>  [DeltaSync.Network]  ──>  [DeltaSync.Core]
      │                                                ▲
      └────────────────────────────────────────────────┘
```
1. `DeltaSync.Core` has **zero** dependencies on external transport, networking frameworks, or CLI packages.
2. `DeltaSync.Network` references only `DeltaSync.Core`.
3. Infrastructure concerns (SQLite, UDP sockets, Spectre.Console, Prometheus) reside on the outer rings or adapt core ports.

---

## Ports the Core Owns

Core defines and owns the abstractions for all external interactions:
1. **`IPeerTransportChannel`:** Asynchronous frame/message transport between peers (`SendAsync`, `ReceiveAsync`).
2. **`ISqliteStateStore`:** Persistence port for metadata, chunks, and Merkle prefix nodes.
3. **`ILocalChunkProvider`:** Local disk block retrieval port for chunk serving.
4. **`IFileWatcherService`:** Local filesystem change notification port (`FileCreated`, `FileChanged`, `FileDeleted`, `FileRenamed`).
5. **`ISyncWireProtocol`:** Wire protocol message handler port (`HandleMessageAsync`, `ExchangePrefixesAsync`).
6. **`ISyncMetricsSink`:** Observability port for telemetry emissions (`RecordBytesSent`, `RecordBytesReceived`, `RecordConflictDetected`, `RecordSyncDuration`).

---

## Deferred Decisions

1. **Selective Sync Rules:** Glob pattern filtering for file exclusion (deferred until full-tree sync is hardened).
2. **Transport Encryption:** TLS/Noise protocol encryption over raw TCP channels (deferred; current wire abstraction operates over any `IPeerTransportChannel`).
3. **Block-Level Compression:** LZ4/Zstandard chunk compression over the wire (deferred; FastCDC delta transfer already eliminates unmodified chunks).

---

## Fitness Functions

Enforced mechanically via `tests/DeltaSync.Tests/Architecture/ArchitectureFitnessTests.cs`:
1. **Layer Dependency Verification:**
   - `DeltaSync.Core` does not reference `DeltaSync.Network` or `DeltaSync.Cli`.
   - `DeltaSync.Network` does not reference `DeltaSync.Cli`.
2. **Circular Dependency Freedom:**
   - Assembly dependency graph is strictly acyclic.
3. **Purity of Core Causality and Chunking:**
   - Types under `DeltaSync.Core.Causality` and `DeltaSync.Core.Chunking` do not reference `Microsoft.Data.Sqlite` or `System.Net.Sockets`.
4. **Port Decoupling:**
   - External dependencies (such as network channels) communicate with Core solely through core-owned interface ports.

---

## Stress Test
See detailed 3-scenario stress test in [docs/architecture/deltasync-stress.md](deltasync-stress.md).

---

## Open Structural Questions
None. All 5 architectural decisions (ADR-0001 through ADR-0005) are decided and in force.
