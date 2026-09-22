# ADR-0003: Embedded SQLite for Sync State and Chunk Indexing

- **Status:** accepted
- **Date:** 2026-09-22
- **Card:** [#4 Where should DeltaSync store its local file index and sync history?](https://github.com/Sehaan-1/DeltaSync/issues/4)
- **Board:** [#1 DeltaSync: Peer-to-Peer File Synchronization Engine](https://github.com/Sehaan-1/DeltaSync/issues/1)
- **Supersedes:** none
- **Superseded by:** none

## Context
DeltaSync must maintain local file metadata, Merkle tree nodes, chunk fingerprints, and vector clock histories reliably across process restarts and crashes. We evaluated:
- Option A: Embedded SQLite database in `.deltasync/state.db`.
- Option B: Flat JSON and binary journal files in `.deltasync/`.

## Decision
Store sync state, chunk fingerprints, and version vectors in an embedded SQLite database inside the `.deltasync` folder.

## Consequences
- **People notice:** Robust local metadata with zero server setup; process kills during writes do not corrupt the database.
- **Later cards/ADRs must:** Use `Microsoft.Data.Sqlite` with WAL mode and atomic transactions for state mutations.
- **We give up:** Direct human inspection of raw JSON files with plain text editors without SQLite tools.
- **Look/CI/proof:** Automated test `Database_CrashSafety_IntegrityCheck_Test` verifying `PRAGMA integrity_check` passes after simulated sudden termination.

## History
- 2026-09-22 accepted
