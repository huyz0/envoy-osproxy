# evoxy-module

The **reference** dynamic-module cdylib: the "works out of the box" `.so`, running
the reference tenancy. It is one line over
[`evoxy-module-sdk`](../evoxy-module-sdk):

```rust
evoxy_module_sdk::register!(evoxy_module_sdk::reference_router);
```

A stock Envoy loads the built `libevoxy_module.so`; the reference router reads its
placement (dedicated cluster or shared index) from Envoy's `filter_config` blob.

To build your **own** module (with your tenancy) you write the same one line with
your factory instead of `reference_router`, so you do not fork this crate. See
[`evoxy-module-sdk`](../evoxy-module-sdk) and
[`examples/custom-module`](../../examples/custom-module).

## Build

Needs `clang` + `libclang` (the SDK binds Envoy's C ABI via `bindgen`). From the
repo root:

```sh
cargo xtask module-image   # builds the .so and bakes it into a stock Envoy image
```

or directly:

```sh
cd crates/evoxy-module
cargo build --release       # target/release/libevoxy_module.so
```

## Why this crate is excluded from the workspace

Through `evoxy-module-sdk` it links the upstream Envoy SDK (a git dependency needing
`libclang`), so `cargo xtask ci` does not build it. The brain it drives,
[`evoxy-filter`](../evoxy-filter), is gated and fully tested without Envoy; the built
`.so` is verified live by `crates/evoxy-extproc/tests/e2e_module.rs`.

## The SDK and Envoy versions must match

Envoy verifies an ABI-header hash at load, so the SDK git tag (in
`evoxy-module-sdk/Cargo.toml`) must equal the Envoy image tag. `tag = "v1.37.0"`
pairs with `envoyproxy/envoy:v1.37.0`; the [`docker/Dockerfile`](docker/Dockerfile)
uses that base and drops the `.so` into its dynamic-modules search path. No fork, no
rebuild of Envoy.
