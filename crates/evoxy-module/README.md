# evoxy-module — the Envoy dynamic-module cdylib

The thin artifact that binds Envoy's dynamic-module C ABI and adapts it to
[`evoxy-filter`](../evoxy-filter)'s SDK-agnostic `EnvoyActions` (ADR-004). Envoy
loads the built `.so`; per request it calls the filter callbacks, which this
crate maps to `EnvoyActions`, letting the tested brain do the work.

## Why this crate is workspace-excluded

It depends on `envoy-dynamic-modules-rust-sdk`, which uses `bindgen` (needs
`libclang`) and binds Envoy's ABI headers — a heavy, environment-specific build.
Per ADR-004 it is **not** a workspace member and is **not** built by
`cargo xtask ci`, exactly like osproxy's excluded `osproxy-kafka-rdkafka`. The
brain it drives (`evoxy-filter`) *is* gated and fully tested without Envoy.

## Build prerequisites (host only)

- `libclang` / `clang` (for `bindgen`).
- The Envoy dynamic-modules ABI headers the SDK expects (per the SDK's docs for
  the Envoy version you target — we track latest, ADR-001).
- The pinned Rust toolchain (`rust-toolchain.toml` at the repo root).

```sh
cd crates/evoxy-module
cargo build --release        # produces target/release/libevoxy_module.so
```

## Integration contract (what `src/lib.rs` wires)

The non-SDK wiring is in [`src/lib.rs`](src/lib.rs) and is written against
`evoxy-filter` only, so it is reviewable without the SDK:

1. **Init** — parse Envoy's `filter_config` blob into `FilterConfig`, build the
   tenancy (default: `ReferenceTenancy`; a user artifact swaps this via the
   `register!` factory — ADR-003), wrap it in a `TenancyRouter`, and construct a
   `Filter`. Capture a Tokio runtime `Handle`.
2. **Per request** — buffer request headers + body into an
   `evoxy_abi::FilterRequest`, then `block_on` `Filter::handle(&req, &mut acts)`,
   where `acts` is an `EnvoyActions` implemented over the SDK's request handle.
3. **Apply** — `ContinueUpstream` ⇒ return the SDK's "continue" status so Envoy
   forwards the mutated request to the selected cluster; `StoppedWithLocalReply`
   ⇒ the local reply was already emitted, return the SDK's "stop" status.

### The SDK seam — implemented in `src/sdk.rs`

`src/sdk.rs` (behind `--features sdk`) is the real binding:
- `init!(new_http_filter)` registers the module entry point; `new_http_filter`
  builds the reference tenancy + a runtime.
- `EvoxyHttpFilter`/`EvoxyInstance` implement the SDK's `HttpFilter`/
  `HttpFilterInstance`: buffer the needed headers at the header phase, run the
  brain (`Module::on_request`) at the body phase, and apply the effects.
- `SdkActions` implements `EnvoyActions` as an owned recorder (so it stays `Send`)
  and commits body replacement / a fail-closed `send_response` to the Envoy handle.

**Verified:** `cargo build --release --features sdk` produces
`target/release/libevoxy_module.so`, which exports the full
`envoy_dynamic_module_event_*` ABI (`nm -D` confirms `..._program_init`,
`..._request_headers`, `..._request_body`, …) — a loadable Envoy dynamic module.

**Known SDK-0.1.x limitation** (documented in `sdk.rs`): the request headers map
is only reachable in the header phase and cannot be enumerated, so routing/header
rewrites (multi-cluster, physical-index remap) need the header-phase split (M2),
exactly like the ext_proc backend. The reference-tenancy default artifact routes
statically and needs only body mutation + fail-closed reply, which this seam does.
An end-to-end test through a real Envoy loading the `.so` is future work (parallels
the ext_proc `tests/e2e.rs`).
