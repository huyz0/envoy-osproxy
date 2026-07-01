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

### The one SDK seam

The only SDK-specific code is:
- implementing `EnvoyActions` over the SDK request handle (set `:method`/`:path`,
  replace the body buffer, set/remove headers, `send_local_reply`, and select the
  upstream cluster), and
- the SDK's module/filter registration macro invoking our `register!` factory.

These are marked `SDK:` in `src/lib.rs`. They are host-gated (needs the SDK) and
must be verified on a build host — this environment has no `libclang`, so they are
written to the SDK's documented API but not compiled here (ADR-004). Everything
else is exercised by `evoxy-filter`'s tests.
