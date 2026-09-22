# ADR-0002: Hybrid Local Broadcast and Static Peer Discovery

- **Status:** accepted
- **Date:** 2026-09-22
- **Card:** [#3 How do two devices find each other to start syncing?](https://github.com/Sehaan-1/DeltaSync/issues/3)
- **Board:** [#1 DeltaSync: Peer-to-Peer File Synchronization Engine](https://github.com/Sehaan-1/DeltaSync/issues/1)
- **Supersedes:** none
- **Superseded by:** none

## Context
DeltaSync nodes need to discover and connect to peers without requiring centralized cloud servers. We evaluated:
- Option A: Local network discovery (mDNS / UDP broadcast) only.
- Option B: Explicit peer address list only.
- Option C: Hybrid: local broadcast with static peer address fallback.

## Decision
Use automatic local broadcast discovery by default, but allow entering peer IP addresses for remote or test nodes.

## Consequences
- **People notice:** Machines on the same Wi-Fi connect automatically with zero setup, while automated tests and cross-subnet machines connect using `--peer <ip>:<port>`.
- **Later cards/ADRs must:** Provide an `IPeerDiscovery` abstraction that unifies broadcast beacons and configured endpoints into a single peer-event stream.
- **We give up:** The simplicity of having only one network discovery code path.
- **Look/CI/proof:** Automated integration test `PeerDiscovery_LocalAndStatic_Test`.

## History
- 2026-09-22 accepted
