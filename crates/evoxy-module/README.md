# evoxy-module — the Envoy dynamic-module cdylib

The thin artifact that binds Envoy's dynamic-module C ABI and adapts it to
[`evoxy-filter`](../evoxy-filter)'s SDK-agnostic `EnvoyActions` (ADR-004). Envoy
loads the built `.so`; per request it calls the filter callbacks, which this
crate maps to `EnvoyActions`, letting the tested brain do the work.

## Why this crate is workspace-excluded

It depends on the OFFICIAL upstream `envoy-proxy-dynamic-modules-rust-sdk` (which
lives in the `envoyproxy/envoy` tree), which uses `bindgen` (needs `libclang`) and
binds Envoy's ABI headers — a heavy, environment-specific build. Per ADR-004 it is
**not** a workspace member and is **not** built by `cargo xtask ci`, exactly like
osproxy's excluded `osproxy-kafka-rdkafka`. The brain it drives (`evoxy-filter`)
*is* gated and fully tested without Envoy.

## SDK ⇔ Envoy version pinning

Envoy verifies the ABI-header hash at load: `envoy_dynamic_module_on_program_init`
returns the SDK's `kAbiVersion`, which must equal Envoy's own. So the SDK git tag
in `Cargo.toml` **must equal** the Envoy image tag — a bump is a deliberate, paired
event (SDK `tag = "v1.37.0"` ⇔ `envoyproxy/envoy:v1.37.0`).

## Build prerequisites (host only)

- `libclang` / `clang` (for `bindgen`).
- The pinned Rust toolchain (`rust-toolchain.toml` at the repo root).
- glibc no newer than the target Envoy image's (build in an older-or-equal distro;
  the `docker/Dockerfile` uses Debian bookworm, glibc 2.36 < the image's 2.39).

```sh
cd crates/evoxy-module
cargo build --release --features sdk   # produces target/release/libevoxy_module.so
```

Or, reproducibly, build the module **and** bake it into a stock Envoy image in one
step (run from `~/work`, the parent of both repos):

```sh
docker build -f envoy-osproxy/crates/evoxy-module/docker/Dockerfile -t evoxy-envoy:v1.37.0 .
```

## Integration contract (what `src/lib.rs` wires)

The non-SDK wiring is in [`src/lib.rs`](src/lib.rs) and is written against
`evoxy-filter` only, so it is reviewable without the SDK:

1. **Init** — parse Envoy's `filter_config` blob into `FilterConfig`, build the
   tenancy (default: `ReferenceTenancy`; a user artifact swaps this — ADR-003),
   wrap it in a `TenancyRouter`, and construct a `Filter`. Capture a Tokio runtime
   `Handle`.
2. **Per request** — enumerate request headers at the header phase, buffer the body,
   assemble an `evoxy_abi::FilterRequest`, then `block_on` `Filter::handle`, where
   `acts` is an `EnvoyActions` recorder.
3. **Apply** — `ContinueUpstream` ⇒ commit the recorded header/path/body mutations
   and continue so Envoy forwards to the upstream; `StoppedWithLocalReply` ⇒ emit
   the fail-closed `send_response`.

### The SDK seam — implemented in `src/sdk.rs`

`src/sdk.rs` (behind `--features sdk`) is the real binding against the official SDK:
- `declare_init_functions!(init, new_http_filter_config_fn)` registers the module;
  `new_http_filter_config_fn` matches `filter_name` (`evoxy`) and builds the
  reference tenancy + a runtime.
- `EvoxyConfig`/`EvoxyFilter` implement the SDK's `HttpFilterConfig`/`HttpFilter`:
  capture the (enumerable) headers at the header phase, run the brain
  (`Module::on_request`) at the body phase (or at the header phase for a body-less
  read), and apply the effects.
- `SdkActions` implements `EnvoyActions` as an owned recorder (so it stays `Send`)
  and commits `:method`/`:path`/header mutations, body drain+append, and a
  fail-closed `send_response` to the Envoy handle.

**Verified live (not estimated):** the built `libevoxy_module.so` is loaded by a
**stock, unmodified `envoyproxy/envoy:v1.37.0`** (the ABI hash matches — Envoy
accepts the `DynamicModuleFilter` config and reaches its dispatch loop with no
rejection), and driven end-to-end against a real OpenSearch by the 3-leg latency
harness `evoxy-extproc/tests/perf_module.rs`. Measured: the in-process module adds
**no milliseconds over Envoy** (its cost is below Envoy's own proxying jitter),
versus the ext_proc backend's measured **+2.3 ms** out-of-process hop — see
[docs/12](../../docs/12-backend-comparison.md).

Unlike the earlier prototype ABI, this SDK **can** enumerate + mutate the request
header map and the body buffer, so the module applies the *full*
transform-then-forward (path rewrite, header inject, body splice, fail-closed
reply). Cluster override is not exposed by this SDK rev; the reference tenancy
static-routes to the one configured upstream, so `set_upstream_cluster` is recorded
but not applied (the physical-index rewrite rides on `set_path`).
