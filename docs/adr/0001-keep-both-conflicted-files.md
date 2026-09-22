# ADR-0001: Keep Both Conflicted Files Side-by-Side

- **Status:** accepted
- **Date:** 2026-09-22
- **Card:** [#2 When two devices edit the same file while disconnected, what happens automatically?](https://github.com/Sehaan-1/DeltaSync/issues/2)
- **Board:** [#1 DeltaSync: Peer-to-Peer File Synchronization Engine](https://github.com/Sehaan-1/DeltaSync/issues/1)
- **Supersedes:** none
- **Superseded by:** none

## Context
In a distributed peer-to-peer sync engine, two machines can disconnect, edit the same file concurrently, and reconnect. We needed to choose how DeltaSync resolves conflicting revisions without data loss. We evaluated:
- Option A: Keep both files side-by-side with device names added (`file (MachineName conflicted).ext`).
- Option B: Last-writer-wins with automatic hidden backup.
- Option C: Pause sync on the conflicted file until interactive manual CLI resolution.

## Decision
If two machines change the same file offline, keep both copies side-by-side with device names attached to prevent silent data loss.

## Consequences
- **People notice:** Both versions remain visible in their synced folder; no work is ever silently overwritten or moved out of sight.
- **Later cards/ADRs must:** Implement vector clock causality checking to detect true concurrent forks, safe side-by-side renaming, and propagating the newly created conflicted copy back to peers.
- **We give up:** Fully automatic single-file convergence without human review.
- **Look/CI/proof:** Automated integration test `ConflictResolution_BothCopiesPreserved_Test` verifying both files exist with bit-for-bit accuracy.

## History
- 2026-09-22 accepted
