# envoy-osproxy

**Multi-tenant OpenSearch proxy capabilities delivered as an extension of a stock
Envoy** — without forking, patching, or recompiling Envoy. Point a standard
`envoyproxy/envoy` release at your OpenSearch cluster, load one artifact, and get
per-tenant isolation, request/response reshaping, `_bulk`/`_mget`/`_msearch` demux,
epoch-gated migration, shape-only observability, and async fan-out.

## Two backends, one brain

The same request-handling logic runs behind either Envoy extension seam — a
deployment knob, not a rewrite (see the [Backend Comparison](12-backend-comparison.md)):

| backend | mechanism | measured added latency | trade-off |
|---|---|---|---|
| **ext_proc** | out-of-process gRPC sidecar | **+2.3 ms** over Envoy | process isolation, independent deploy |
| **dynamic module** | in-process Rust `.so` (upstream `dynamic_modules`) | **≈ 0 ms** over Envoy (in the noise) | lowest latency, shared crash domain |

Both are verified live end-to-end through a **stock, unmodified** Envoy — the
module is loaded via the upstream `DynamicModuleFilter` (no fork, no rebuild).

## The engine

The request brain reuses the transport-agnostic **osproxy engine crates**
(`osproxy-core`/`-spi`/`-tenancy`/`-rewrite`), pulled from **crates.io** — a
`cargo build` resolves everything; there is no other repository to check out. The
`evoxy-*` crates are the Envoy-facing layer that builds the same `RequestCtx` the
standalone proxy builds and drives the same engine behind Envoy's seams.

## Where to start

- [Technical Analysis](00-technical-analysis.md) — the approach, capability
  mapping, boundary shifts, and milestone plan.
- [Architecture](01-architecture.md) — the crate map and the request path.
- [Backend Comparison](12-backend-comparison.md) — ext_proc vs. dynamic module,
  benchmark-grounded.
- [Architecture Decisions](decisions/README.md) — the ADR trail.
