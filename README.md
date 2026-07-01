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

## Built on osproxy

This project **builds on top of [osproxy](https://github.com/huyz0/opensearch-proxy)**,
the standalone OpenSearch proxy. It reuses osproxy's transport-agnostic engine
crates (`osproxy-core`/`-spi`/`-tenancy`/`-rewrite`) from **crates.io** (pinned
`=1.0.1`) — so a `cargo build` resolves everything and there is no other repository
to check out. **osproxy owns the multi-tenant OpenSearch logic; this project hosts
that logic inside a stock Envoy** instead of osproxy's own HTTP server:

| | osproxy (reused engine) | envoy-osproxy (this repo) |
|---|---|---|
| tenancy, placement, reshaping, `_bulk`/`_mget`/`_msearch`, migration | ✅ owns it | reuses as-is |
| the wire: HTTP, TLS/mTLS, pooling, LB, retries | osproxy's own server | **Envoy** (we ship none) |
| how the brain is invoked | osproxy's pipeline | Envoy **ext_proc** or **dynamic module** |

The `evoxy-*` crates are the thin Envoy-facing layer that builds the same
`RequestCtx` and drives the same engine behind Envoy's seams.

## Not turnkey — how to run it

This is a **toolkit, not a ready-to-run proxy.** To put it in front of OpenSearch
you (1) implement the tenancy SPI *or* use the built-in reference tenancy, (2) build
an artifact — an ext_proc server or a dynamic-module `.so`, and (3) write the Envoy
bootstrap. See **[examples/](examples)** for compiling SPI code and Envoy configs
for both backends.

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
| [examples/](examples) | A compiling custom `TenancySpi` + Envoy configs for both backends |
| [xtask](xtask) | The gate (`cargo xtask ci`) and image builder |
| [docs/](docs) | Spec-driven docs and ADRs |

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

## Documentation

The user guide — introduction, architecture (with diagrams), usage, and the
backend comparison — is published at **https://huyz0.github.io/envoy-osproxy/**
(source in [docs/guide/](docs/guide)). Internal design notes and decision records
live under [docs/](docs).

## License

Apache-2.0.
