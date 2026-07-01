# envoy-osproxy

A sister project of [`opensearch-proxy`](../opensearch-proxy) (osproxy). Same
capabilities — multi-tenant isolation, body reshaping, `_bulk` demux,
epoch-gated migration, shape-only observability with runtime directives, traffic
capture — but delivered as an **extension of a stock Envoy**, without forking or
recompiling Envoy.

**Why it's tractable:** osproxy already split the wire (`osproxy-transport`) from
the brain (`osproxy-engine::Pipeline::handle(RequestCtx) -> PipelineResponse`).
Envoy replaces the transport; the existing, tested engine crates become the
plug-in logic behind Envoy's `ext_proc` (out-of-process gRPC) seam, with a
dynamic-module (in-process Rust `.so`) fast path as a later drop-in.

## Status

**M0 (walking skeleton) done** — the Envoy→engine seam is built, tested, and
benchmarked. Given any Envoy request, `evoxy-adapter` builds the exact
`RequestCtx` the standalone proxy builds, reusing the osproxy engine crates by
path. Full `cargo xtask ci` gate, git hooks, spec-driven docs, and an AI
quality-review agent are in place. See [docs/11-roadmap.md](docs/11-roadmap.md).

### Layout

| Path | What |
|------|------|
| [crates/evoxy-abi](crates/evoxy-abi) | Envoy-facing wire model (`FilterRequest`/`FilterResponse`/`MtlsIdentity`) |
| [crates/evoxy-adapter](crates/evoxy-adapter) | The seam: `FilterRequest` → `osproxy_spi::RequestCtx` |
| [xtask](xtask) | The gate (`cargo xtask ci`) |
| [docs/](docs) | Spec-driven docs, ADRs, roadmap |

### Read here first

- [docs/00-technical-analysis.md](docs/00-technical-analysis.md) — the approach:
  no-rebuild extension mechanisms, capability mapping, boundary shifts
  (FIPS→Envoy-BoringSSL, tracing, admin plane), performance, milestone plan.
- [docs/01-architecture.md](docs/01-architecture.md) · [AGENTS.md](AGENTS.md) ·
  [docs/decisions/](docs/decisions) (ADRs).

### Develop

```
scripts/setup-hooks.sh   # once: install the commit + pre-commit gate
cargo xtask ci           # fmt, clippy, arch, test, doc, budgets
cargo xtask bench        # iai-callgrind microbenchmarks (needs valgrind)
```

## Non-negotiable: no Envoy rebuild

Operators run an unmodified Envoy release. Our logic ships as bootstrap config
plus a loadable artifact (an `ext_proc` service container, and later a
`dynamic_modules` `.so`). We never patch Envoy source.
