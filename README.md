# DeltaSync

> Peer-to-Peer File Synchronization Engine with Delta Transfer, Conflict Detection, and Chaos-Tested Crash Safety.

## Overview
DeltaSync is a cross-device synchronization engine designed to keep folders identical across machines with delta transfer, vector clock conflict resolution, and crash-safe reliability guarantees.

## Architecture
- `src/DeltaSync.Core`: Core domain models, Merkle tree index, chunking engines, vector clocks, and local state repository.
- `src/DeltaSync.Network`: Transport layer, gRPC/Protobuf streaming contracts, and peer discovery services.
- `src/DeltaSync.Cli`: Command-line interface with Spectre.Console real-time status and observability endpoints.
- `tests/DeltaSync.Tests`: Unit, property-based (FsCheck), and integration test suites.

## Build and Run
```bash
# Build
dotnet build

# Run Tests
dotnet test

# Run CLI
dotnet run --project src/DeltaSync.Cli
```
