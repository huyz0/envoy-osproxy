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

## Built on osproxy

This project **builds on top of [osproxy](https://github.com/huyz0/opensearch-proxy)**
(the standalone OpenSearch proxy). osproxy already split its *wire* (its own HTTP
server) from its *brain* (a transport-agnostic engine: `RequestCtx → decision`).
envoy-osproxy reuses that brain unchanged — the engine crates
`osproxy-core`/`-spi`/`-tenancy`/`-rewrite` are pulled from **crates.io** (pinned
`=1.0.1`), so a `cargo build` resolves everything and there is no other repository
to check out. The `evoxy-*` crates are the thin Envoy-facing layer that builds the
same `RequestCtx` and drives the same engine behind Envoy's extension seams.

### What osproxy provides vs. what this project adds

| concern | osproxy (the reused engine) | envoy-osproxy (the Envoy adaptation) |
|---|---|---|
| tenancy/placement logic | the `TenancySpi` + routing engine | reused as-is |
| body/response reshaping, `_bulk`/`_mget`/`_msearch`, id map/unmap | the transform engine | reused as-is |
| isolation model, epoch-gated migration | defined here | reused as-is |
| **the wire** (HTTP, TLS/mTLS, pooling, LB, retries, circuit breaking) | osproxy's own server + client | **Envoy** — we ship none of it |
| **how the brain is invoked** | osproxy's pipeline | an Envoy **ext_proc** service or **dynamic module** (ADR-001/004) |
| observability surfaces | osproxy's | re-expressed on Envoy's port (`/metrics`, `/debug/*`, decision header) |
| async fan-out | osproxy's sink | Envoy request-mirror + a bridge (ADR-005) |

In short: **osproxy owns the multi-tenant OpenSearch logic; envoy-osproxy hosts it
inside a stock Envoy** instead of a bespoke server.

## Not turnkey

This is a **toolkit, not a ready-to-run proxy**. To deploy it you implement the
tenancy SPI (or use the built-in reference tenancy), build an artifact (an ext_proc
server or a dynamic-module `.so`), and write the Envoy bootstrap. The
[examples](https://github.com/huyz0/envoy-osproxy/tree/main/examples) walk through
all three with compiling code and Envoy configs.

## Where to start

- [Technical Analysis](00-technical-analysis.md) — the approach, capability
  mapping, boundary shifts, and milestone plan.
- [Architecture](01-architecture.md) — the crate map and the request path.
- [Backend Comparison](12-backend-comparison.md) — ext_proc vs. dynamic module,
  benchmark-grounded.
- [Architecture Decisions](decisions/README.md) — the ADR trail.
