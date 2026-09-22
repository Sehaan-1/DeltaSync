# ADR-0004: FastCDC Content-Defined Chunking for Delta Transfer

- **Status:** accepted
- **Date:** 2026-09-22
- **Card:** [#5 How should DeltaSync break files into chunks for delta transfer?](https://github.com/Sehaan-1/DeltaSync/issues/5)
- **Board:** [#1 DeltaSync: Peer-to-Peer File Synchronization Engine](https://github.com/Sehaan-1/DeltaSync/issues/1)
- **Supersedes:** none
- **Superseded by:** none

## Context
DeltaSync needs to transmit only modified portions of files across the network. Fixed-size block chunking fails under byte insertions due to boundary shift. Finding [#8](https://github.com/Sehaan-1/DeltaSync/issues/8) confirmed that FastCDC achieves >1.8 GB/s in .NET 8 with zero hot-loop allocations and eliminates boundary shifts. We evaluated:
- Option A: Fixed-size blocks with rolling checksums (rsync algorithm).
- Option B: Content-Defined Chunking (FastCDC).

## Decision
Use Content-Defined Chunking (FastCDC) with rolling Gear hashes and subchunk normalization so that byte insertions do not shift block boundaries across the file.

## Consequences
- **People notice:** Modifying 3 lines anywhere in a 50MB file transfers <128KB (>99.7% bandwidth reduction).
- **Later cards/ADRs must:** Implement FastCDC algorithm with configurable min (16KB), target (64KB), and max (256KB) chunk bounds and SHA-256 / BLAKE3 chunk hashes.
- **We give up:** The simple fixed-grid math of fixed block offsets.
- **Look/CI/proof:** Automated benchmark test `DeltaTransfer_BandwidthSavings_Assertion_Test` asserting >99% bandwidth savings on prepended and in-place edits.

## History
- 2026-09-22 accepted
