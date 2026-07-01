# envoy-osproxy

Multi-tenant **OpenSearch proxy capabilities delivered as an extension of a stock
Envoy** — without forking, patching, or recompiling Envoy. Point a standard
`envoyproxy/envoy` release at your OpenSearch cluster, load one artifact, and get:

- **multi-tenant isolation** — per-tenant partitioning over shared or dedicated
  indices, enforced on every read and write;
- **request/response reshaping** — inject the tenant field, construct
  partition-scoped document ids, wrap queries with the mandatory partition filter,
  and unmap it all on the way back;
- **`_bulk` / `_mget` / `_msearch` demux** with bounded memory (413/429 caps);
- **epoch-gated migration** — a fleet-safe write gate (stale-epoch ⇒ 409);
- **shape-only observability** — `/metrics`, `/debug/explain`, a decision header,
  W3C trace propagation, and runtime diagnostics directives that leak no tenant
  data;
- **async fan-out** to a capture bridge via Envoy's request-mirror.

## Two backends, one brain

The same request-handling logic runs behind either Envoy extension seam — it's a
deployment knob, not a rewrite (see [docs/12](docs/12-backend-comparison.md)):

| backend | mechanism | measured added latency | trade-off |
|---|---|---|---|
| **ext_proc** | out-of-process gRPC sidecar | **+2.3 ms** over Envoy | process isolation, independent deploy, no build toolchain |
| **dynamic module** | in-process Rust `.so` (upstream `dynamic_modules`) | **≈ 0 ms** over Envoy (in the noise) | lowest latency, shared crash domain |

Both are verified live end-to-end through a **stock, unmodified** Envoy — the
module is loaded via the upstream `DynamicModuleFilter` (no fork, no rebuild).

## The engine

The request brain reuses the transport-agnostic **osproxy engine crates**
(`osproxy-core`/`-spi`/`-tenancy`/`-rewrite`), pulled from **crates.io** — so a
`cargo build` resolves everything; there is no other repository to check out or be
aware of. The `evoxy-*` crates here are the Envoy-facing layer: they build the
same `RequestCtx` the standalone proxy builds and drive the same engine behind
Envoy's seams instead of a bespoke HTTP server.

### Layout

| Path | What |
|------|------|
| [crates/evoxy-abi](crates/evoxy-abi) | Envoy-facing wire model (`FilterRequest`/`FilterResponse`/`MtlsIdentity`) |
| [crates/evoxy-adapter](crates/evoxy-adapter) | The seam: `FilterRequest` → `RequestCtx` |
| [crates/evoxy-route](crates/evoxy-route) | Transform-then-forward routing (ADR-002) |
| [crates/evoxy-filter](crates/evoxy-filter) | The SDK-agnostic filter brain |
| [crates/evoxy-extproc](crates/evoxy-extproc) | The ext_proc gRPC backend |
| [crates/evoxy-module](crates/evoxy-module) | The dynamic-module cdylib (workspace-excluded) |
| [crates/evoxy-bridge](crates/evoxy-bridge) | The async fan-out HTTP→Kafka bridge |
| [crates/evoxy-bench](crates/evoxy-bench) | Pure NFR-P bench substrate (dev-only) |
| [xtask](xtask) | The gate (`cargo xtask ci`) and image builder |
| [docs/](docs) | Spec-driven docs, ADRs, roadmap |

## Develop

```sh
scripts/setup-hooks.sh   # once: install the commit + pre-commit gate
cargo xtask ci           # fmt, clippy, arch, test, doc, budgets
cargo xtask bench        # iai-callgrind microbenchmarks (needs valgrind)
cargo xtask module-image # build the dynamic module into a stock Envoy image (Docker)
```

The Docker-gated live tests (real Envoy + OpenSearch) are `#[ignore]`'d; run them
with `cargo test -p evoxy-extproc -- --ignored`.

## Non-negotiable: no Envoy rebuild

Operators run an unmodified Envoy release. Our logic ships as bootstrap config
plus a loadable artifact (an `ext_proc` service container, or a `dynamic_modules`
`.so`). We never patch Envoy source.

## Read here first

- [docs/00-technical-analysis.md](docs/00-technical-analysis.md) — the approach:
  no-rebuild extension mechanisms, capability mapping, boundary shifts, milestones.
- [docs/01-architecture.md](docs/01-architecture.md) ·
  [docs/12-backend-comparison.md](docs/12-backend-comparison.md) ·
  [docs/decisions/](docs/decisions) (ADRs).

## License

Apache-2.0.
