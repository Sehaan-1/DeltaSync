---
title: DeltaSync Knowledge Hub
tags:
  - deltasync
  - architecture
  - distributed-systems
  - p2p
  - moc
status: active
canvas: "[[DeltaSync Architecture.canvas]]"
---

# DeltaSync Architecture & Knowledge Hub

Welcome to the **DeltaSync** knowledge vault. DeltaSync is a peer-to-peer file synchronization engine designed for LAN and WAN networks with content-defined chunking, vector-clock causality, and deterministic conflict resolution.

> [!tip] Interactive Canvas Available
> Open the visual architecture map: **[[DeltaSync Architecture.canvas]]** to explore subsystems, data flows, and cross-cutting connections visually.

---

## 🏛️ Architectural Decision Records (ADRs)

| ADR | Title | Decision Summary | Status |
| :--- | :--- | :--- | :--- |
| **[[0001-keep-both-conflicted-files\|ADR-0001]]** | Keep Both Conflicted Files | On concurrent updates, write side-by-side branch files (`<name>.sync-conflict-<ts>-<peer>.<ext>`) without data loss. | `accepted` |
| **[[0002-hybrid-local-and-static-peer-discovery\|ADR-0002]]** | Hybrid Peer Discovery | UDP multicast beacons for zero-config LAN discovery combined with static addresses & backoff for WAN. | `accepted` |
| **[[0003-sqlite-state-store\|ADR-0003]]** | SQLite State Store | Embedded SQLite with WAL mode for file metadata, chunk inverted indexing, and ref-count maintenance. | `accepted` |
| **[[0004-fastcdc-chunking\|ADR-0004]]** | FastCDC Chunking | Content-defined chunking via Gear Hashing (`Min=16KB, Avg=64KB, Max=256KB`) for shift-invariant delta transfer. | `accepted` |
| **[[0005-observability-terminal-and-prometheus\|ADR-0005]]** | Observability & Dashboard | Live Spectre.Console terminal status grid + Prometheus `/metrics` endpoint for real-time monitoring. | `accepted` |

---

## 🔬 Core Subsystems & Research Dossiers

### 1. Peer Discovery & Networking
- **Dossier**: `docs/research/peer-discovery-protocol.md`
- **Core Components**:
  - `DeltaSync.Network.UdpBeaconAnnouncer` — Periodic UDP multicast frame emitter with jitter.
  - `DeltaSync.Network.UdpBeaconListener` — Subnet packet listener with local loopback suppression.
  - `DeltaSync.Network.PeerRegistry` — Thread-safe peer table with liveness states (`Active`, `Stale`, `Dead`).
  - `DeltaSync.Network.BgpCollisionTieBreaker` — Deterministic collision resolution for simultaneous peer dials.

### 2. Causality & Conflict Detection
- **Dossier**: `docs/research/vector-clock-causality.md`
- **Core Components**:
  - `DeltaSync.Core.Causality.VectorClock` — Compact, immutable version vectors with byte and JSON serialization.
  - `DeltaSync.Core.Causality.ConflictResolver` — Evaluates causal ordering (`Equal`, `Before`, `After`, `Concurrent`).
  - `DeltaSync.Core.Causality.ConflictedPathHelper` — Collision-free side-by-side naming format generation.

### 3. FastCDC Delta Chunking & Reconstruction
- **Dossier**: `docs/research/fastcdc-chunking.md`
- **Findings Benchmark**: `docs/findings/chunking-performance.md`
- **Core Components**:
  - `DeltaSync.Core.Chunking.FastCdcChunker` — Gear hash rolling window for content-defined chunk cuts.
  - `DeltaSync.Core.Chunking.ChunkFingerprinter` — BLAKE3 / SHA-256 chunk integrity hashing.
  - `DeltaSync.Core.Chunking.StreamingFastCdcReader` — Asynchronous streaming chunk generator.
  - `DeltaSync.Core.Chunking.DeltaReconstructor` — Zero-copy chunk stitching against file manifests.

### 4. Persistence & State Sync
- **Dossier**: `docs/research/sqlite-state-store.md`
- **Core Components**:
  - `DeltaSync.Core.Storage.SqliteStateStore` — WAL-mode SQLite database with atomic transaction guarantees.
  - `DeltaSync.Core.Storage.SqliteSchemaMigrator` — Idempotent schema migrations and crash recovery.
  - `DeltaSync.Core.Storage.MerkleTreeHelper` — Prefix-tree root hashing for $O(1)$ difference detection.

---

## 🧭 Navigation & Graph Explorer

> [!info] Two Ways to Explore in Obsidian
> 1. **Project Vault**: Open `c:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\` as an Obsidian vault to browse markdown docs, ADRs, and the interactive canvas at `docs/DeltaSync Architecture.canvas`.
> 2. **Deep Knowledge Graph Vault**: Open `c:\Users\INDIA TECHNOLOGY\Documents\DeltaSync\graphify-out\obsidian\` as an Obsidian vault to explore 1,350+ interconnected symbol and code entity notes with backlinks and `graph.canvas`.
