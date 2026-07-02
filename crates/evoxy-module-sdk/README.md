# evoxy-module-sdk

Build an Envoy **dynamic module** over the evoxy brain. This crate is what your
module `cdylib` depends on: the [`register!`](src/lib.rs) macro, the SDK binding
(generic over any tenancy), and the [`Module`](src/lib.rs) driver.

## Your whole module

```rust
use osproxy_tenancy::TenancyRouter;

evoxy_module_sdk::register!(|config: &str| {
    let tenancy = my_tenancy::MyTenancy::from_json(config);
    TenancyRouter::new(tenancy)
});
```

`register!` takes a factory `fn(&str) -> impl Router` (a non-capturing closure or a
`fn` path) that turns Envoy's `filter_config` blob into a router. It generates
Envoy's `on_program_init` entry point and wires your factory in. Invoke it once at
your crate root, build a `cdylib`, and you have a loadable `.so`. See
[`examples/custom-module`](../../examples/custom-module).

## Why it is a git dependency, not crates.io

It links the OFFICIAL upstream `envoy-proxy-dynamic-modules-rust-sdk`, which lives
in the `envoyproxy/envoy` git tree and binds Envoy's C ABI via `bindgen`. crates.io
forbids git dependencies at **publish** time, so this crate is not published. That
does not affect you: a dynamic module is a `.so` you build and deploy, never a crate
you publish, and building with a git dependency is perfectly fine. Depend on it by
git, pinned to the release tag:

```toml
evoxy-module-sdk = { git = "https://github.com/huyz0/envoy-osproxy", tag = "v0.1.0" }
```

## The SDK and Envoy versions must match

Envoy verifies an ABI-header hash at load: `on_program_init` returns the SDK's
`kAbiVersion`, and Envoy rejects a module whose hash does not match its own. So the
SDK git tag in this crate's `Cargo.toml` must equal the Envoy image tag. Bumping one
means bumping the other (`tag = "v1.37.0"` pairs with `envoyproxy/envoy:v1.37.0`).

## Build prerequisites

`clang` and `libclang` for `bindgen`, the pinned Rust toolchain, and a glibc no
newer than the target Envoy image's (glibc is forward-compatible only). The
reference [`docker/Dockerfile`](../evoxy-module/docker/Dockerfile) builds on Debian
bookworm (glibc 2.36), which loads on the image's Ubuntu 24.04 (glibc 2.39).

## How it wires together

`Module` (pure Rust over [`evoxy-filter`](../evoxy-filter)) holds the `Filter` brain
and a Tokio runtime handle: per request it enumerates the headers, buffers the body,
runs `Filter::handle` on the runtime (`block_on`, since the in-memory placements resolve
without I/O), and applies the recorded effects. The `sdk` module implements the
SDK's `HttpFilterConfig`/`HttpFilter` generic over your router, with `SdkActions` as
an owned `EnvoyActions` recorder (so it stays `Send`) that commits the method, path,
and header mutations, the body drain-and-append with `content-length` re-synced, and
a fail-closed `send_response`. It also reshapes a read's response back to the
client's logical view.

## What is verified

The reference cdylib built on this crate ([`evoxy-module`](../evoxy-module)) is
loaded by a stock, unmodified `envoyproxy/envoy:v1.37.0`, and driven against a real
OpenSearch by `crates/evoxy-extproc/tests/e2e_module.rs` (a shared-index multi-tenant
round-trip exercising both the request and response transform) and `perf_module.rs`
(the three-leg latency comparison). The `custom-module` example is compiled in CI, so
the user path stays green too.
