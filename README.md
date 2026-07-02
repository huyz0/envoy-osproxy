# envoy-osproxy

[![CI](https://github.com/huyz0/envoy-osproxy/actions/workflows/ci.yml/badge.svg)](https://github.com/huyz0/envoy-osproxy/actions/workflows/ci.yml)
[![Docs](https://github.com/huyz0/envoy-osproxy/actions/workflows/docs.yml/badge.svg)](https://github.com/huyz0/envoy-osproxy/actions/workflows/docs.yml)
[![Release](https://github.com/huyz0/envoy-osproxy/actions/workflows/release.yml/badge.svg)](https://github.com/huyz0/envoy-osproxy/actions/workflows/release.yml)
[![User guide](https://img.shields.io/badge/guide-huyz0.github.io%2Fenvoy--osproxy-blue)](https://huyz0.github.io/envoy-osproxy/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Multi-tenant OpenSearch proxy capabilities delivered as an extension of a stock
Envoy, without forking, patching, or recompiling Envoy. Point a standard
`envoyproxy/envoy` release at your OpenSearch cluster, load one artifact, and get
per-tenant isolation, request and response reshaping, `_bulk`/`_mget`/`_msearch`
demux, epoch-gated migration, shape-only observability, and async fan-out.

The user guide is at **https://huyz0.github.io/envoy-osproxy/**.

## Built on osproxy

This project extends [osproxy](https://github.com/huyz0/opensearch-proxy), the
standalone OpenSearch proxy, by running its logic inside Envoy instead of osproxy's
own HTTP server. It reuses osproxy's transport-agnostic engine crates
(`osproxy-core`, `osproxy-spi`, `osproxy-tenancy`, `osproxy-rewrite`) from crates.io,
pinned to `=1.0.2`, so a `cargo build` resolves everything with no other repository
to check out.

osproxy owns the multi-tenant OpenSearch logic. Envoy owns the wire: HTTP, TLS and
mTLS, pooling, load balancing, and retries. The `evoxy-*` crates are the thin layer
between them, building the same request context osproxy builds and driving the same
engine behind Envoy's extension points.

## Two backends, one brain

The same logic runs behind either Envoy extension point. Pick the dynamic module
when latency is the priority; pick ext_proc when process isolation and an
independent deploy are worth a couple of milliseconds.

| backend | mechanism | measured added latency | trade-off |
|---|---|---|---|
| ext_proc | out-of-process gRPC sidecar | +2.3 ms over Envoy | process isolation, independent deploy |
| dynamic module | in-process Rust `.so` | about 0 ms over Envoy (within the noise) | lowest latency, shared crash domain |

Both are verified end to end through a stock, unmodified Envoy. Operators run an
unmodified Envoy release; our logic ships as bootstrap config plus a loadable
artifact. We never patch Envoy source.

## This is a toolkit, not a ready-to-run proxy

There is no binary to run directly. To put envoy-osproxy in front of OpenSearch you
implement a tenancy (or use the built-in reference tenancy), build an artifact (an
ext_proc server or a dynamic-module `.so`), and write the Envoy bootstrap. The
[examples](examples) directory has compiling tenancy code and Envoy configs for
both backends, and the [user guide](https://huyz0.github.io/envoy-osproxy/) walks
through all three steps.

## Layout

| Path | What |
|------|------|
| [crates/evoxy-abi](crates/evoxy-abi) | The Envoy-facing wire model |
| [crates/evoxy-adapter](crates/evoxy-adapter) | The seam: Envoy request to engine request |
| [crates/evoxy-route](crates/evoxy-route) | Transform-then-forward routing |
| [crates/evoxy-filter](crates/evoxy-filter) | The SDK-agnostic filter brain |
| [crates/evoxy-extproc](crates/evoxy-extproc) | The ext_proc gRPC backend |
| [crates/evoxy-module-sdk](crates/evoxy-module-sdk) | Build your own module: the `register!` macro + SDK glue (workspace-excluded) |
| [crates/evoxy-module](crates/evoxy-module) | The reference dynamic-module cdylib (workspace-excluded) |
| [crates/evoxy-bridge](crates/evoxy-bridge) | The async fan-out sink |
| [crates/evoxy-bench](crates/evoxy-bench) | Benchmark math (dev-only) |
| [examples/](examples) | A compiling custom tenancy and Envoy configs for both backends |
| [xtask](xtask) | The gate (`cargo xtask ci`) and the image builder |
| [docs/](docs) | Design notes and decision records |

## Develop

```sh
scripts/setup-hooks.sh   # once: install the commit and pre-commit gate
cargo xtask ci           # fmt, clippy, arch, test, doc, budgets
cargo xtask bench        # instruction-count microbenchmarks (needs valgrind)
cargo xtask module-image # build the dynamic module into a stock Envoy image (Docker)
```

The Docker-gated live tests (real Envoy and OpenSearch) are ignored by default. Run
them with `cargo test -p evoxy-extproc -- --ignored`.

## Release

Pushing a `v*` tag runs the [Release](.github/workflows/release.yml) workflow, which
publishes the reusable library crates to crates.io. There is no prebuilt binary or
image: the dynamic module is a `.so` you build from your own tenancy (see
[evoxy-module-sdk](crates/evoxy-module-sdk)), and the ext_proc backend is a small
binary you build — both embed your tenancy, so there is nothing generic to ship.

## License

Apache-2.0.
