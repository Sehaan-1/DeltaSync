# ADR-0005: Terminal Status Dashboard with Prometheus Metrics Endpoint

- **Status:** accepted
- **Date:** 2026-09-22
- **Card:** [#6 How should sync activity and metrics be presented to the user?](https://github.com/Sehaan-1/DeltaSync/issues/6)
- **Board:** [#1 DeltaSync: Peer-to-Peer File Synchronization Engine](https://github.com/Sehaan-1/DeltaSync/issues/1)
- **Supersedes:** none
- **Superseded by:** none

## Context
DeltaSync requires observable metrics (sync latency p95, bytes saved, transfer throughput, conflict counts) for demonstration and production readiness. We evaluated:
- Option A: Rich terminal dashboard only.
- Option B: Local web UI dashboard only.
- Option C: Dynamic terminal dashboard plus an optional Prometheus metrics endpoint.

## Decision
Provide a live console status view via Spectre.Console and expose an optional Prometheus metrics endpoint for telemetry.

## Consequences
- **People notice:** Instant, responsive CLI terminal animations for local runs, plus standard `/metrics` integration for Prometheus and Grafana.
- **Later cards/ADRs must:** Provide a clean `ITelemetryService` interface decoupling metric emission from rendering sinks.
- **We give up:** Nothing essential.
- **Look/CI/proof:** Automated integration test `Metrics_ScrapeEndpoint_YieldsValidStats_Test`.

## History
- 2026-09-22 accepted
