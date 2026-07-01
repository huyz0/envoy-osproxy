# evoxy-module

The dynamic-module cdylib. It binds Envoy's dynamic-module C ABI and adapts it to
[`evoxy-filter`](../evoxy-filter)'s SDK-agnostic `EnvoyActions`. Envoy loads the
built `.so`, and per request it calls the filter callbacks, which this crate maps to
`EnvoyActions` so the tested brain does the work.

## Why this crate is excluded from the workspace

It depends on the upstream `envoy-proxy-dynamic-modules-rust-sdk`, which lives in the
`envoyproxy/envoy` tree and uses `bindgen` to bind Envoy's ABI headers. That needs
`libclang` and is an environment-specific build, so the crate is not a workspace
member and `cargo xtask ci` does not build it. The brain it drives, `evoxy-filter`,
is gated and fully tested without Envoy.

## The SDK and Envoy versions must match

Envoy verifies an ABI-header hash at load: `envoy_dynamic_module_on_program_init`
returns the SDK's `kAbiVersion`, and Envoy rejects a module whose hash does not match
its own. So the SDK git tag in `Cargo.toml` must equal the Envoy image tag. Bumping
one means bumping the other (`tag = "v1.37.0"` pairs with `envoyproxy/envoy:v1.37.0`).

## Build prerequisites

You need `clang` and `libclang` for `bindgen`, the pinned Rust toolchain from
`rust-toolchain.toml`, and a glibc no newer than the target Envoy image's, because
glibc is forward-compatible only. The provided `docker/Dockerfile` builds on Debian
bookworm (glibc 2.36), which loads on the image's Ubuntu 24.04 (glibc 2.39).

```sh
cd crates/evoxy-module
cargo build --release --features sdk   # produces target/release/libevoxy_module.so
```

To build the module and bake it into a stock Envoy image in one step, run
`cargo xtask module-image` from the repo root.

## How it wires together

The non-SDK wiring is in [`src/lib.rs`](src/lib.rs) and is written against
`evoxy-filter` only, so it is reviewable without the SDK. At init it parses Envoy's
`filter_config` blob, builds the tenancy (the reference tenancy by default; a custom
one is swapped in here), wraps it in a `TenancyRouter`, constructs a `Filter`, and
captures a Tokio runtime handle. Per request it enumerates the headers, buffers the
body, assembles a `FilterRequest`, runs `Filter::handle` on the runtime, and applies
the recorded effects. A routed request continues so Envoy forwards it; a fail-closed
decision emits the reply and stops.

The SDK binding is in [`src/sdk.rs`](src/sdk.rs), behind the `sdk` feature.
`declare_init_functions!` registers the module and its factory. `EvoxyConfig` and
`EvoxyFilter` implement the SDK's `HttpFilterConfig` and `HttpFilter`: capture the
headers at the header phase, run the brain at the body phase (or at the header phase
for a body-less read), and apply the effects. `SdkActions` implements `EnvoyActions`
as an owned recorder, so it stays `Send`, and commits the method, path, and header
mutations, the body drain-and-append, and a fail-closed `send_response` to Envoy.

## What is verified

The built `.so` is loaded by a stock, unmodified `envoyproxy/envoy:v1.37.0`: the ABI
hash matches, Envoy accepts the filter config, and it reaches its dispatch loop. Two
live harnesses drive it against a real OpenSearch. `perf_module.rs` runs the
three-leg latency comparison, and `e2e_module.rs` runs a correctness pass including a
shared-index multi-tenant round-trip that exercises both the request and response
transform. The in-process module adds no measurable milliseconds over Envoy, against
the ext_proc backend's measured 2.3 ms hop.

This SDK can enumerate and mutate the request header map and both body buffers, so
the module applies the full transform on the request (path rewrite, header inject,
body splice with `content-length` re-synced, fail-closed reply) and reshapes the
response back to the client's logical view. One thing it does not do yet: a
per-request cluster override. The reference tenancy routes to one configured
upstream, so `set_upstream_cluster` is recorded but not applied, and the
physical-index rewrite rides on the path instead.
