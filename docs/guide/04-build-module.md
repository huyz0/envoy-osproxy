# Building the dynamic module

The dynamic module runs your logic in-process, as a shared library that a stock
Envoy loads through its dynamic-modules interface. There is no separate service and
no gRPC hop, so this is the lower-latency backend. A crash in the module takes the
Envoy worker with it, so the same no-panic discipline the code follows is
load-bearing here.

This backend is generic over your tenancy, so a custom `TenancySpi` works end to
end.

## Build prerequisites

The module binds Envoy's C ABI through `bindgen`, which needs `clang` and
`libclang`. It also pins the Envoy dynamic-modules SDK to an exact Envoy release,
because Envoy checks an ABI-header hash at load time. The SDK tag in the crate's
`Cargo.toml` must equal the Envoy image tag. Bumping one means bumping the other.

One more constraint: build the `.so` on a glibc no newer than the target Envoy
image's, because glibc is forward-compatible only. The provided Docker build uses
Debian bookworm (glibc 2.36), which loads on the Envoy image's Ubuntu 24.04
(glibc 2.39).

## The one command

The repository builds the module and bakes it into a stock Envoy image in one step:

```sh
cargo xtask module-image
```

This produces `evoxy-envoy:v1.37.0`: an unmodified `envoyproxy/envoy:v1.37.0` with
`libevoxy_module.so` dropped into its dynamic-modules search path. No fork, no
rebuild of Envoy.

## Wiring your tenancy

The module crate is your build template. The reference tenancy is the default; to
run your own, build it in the factory in
[`crates/evoxy-module/src/sdk.rs`](https://github.com/huyz0/envoy-osproxy/tree/main/crates/evoxy-module/src/sdk.rs).
Replace the default module construction with your tenancy:

```rust
use custom_tenancy::TieredTenancy;
use osproxy_tenancy::TenancyRouter;

let tenancy = TieredTenancy {
    partition_header: "x-tenant".to_owned(),
    cluster: "opensearch".to_owned(),
    premium: ["acme".to_owned()].into_iter().collect(),
};
let module = Module::new(TenancyRouter::new(tenancy), runtime.handle().clone());
```

`Module` is generic over the router, so the request and response transform work the
same for any tenancy. Rebuild the image after any change; the tenancy is compiled
into the `.so`, not loaded at runtime.

## Configuring Envoy

Load the module with the `dynamic_modules` HTTP filter and route to OpenSearch. The
image already sets the search path, so the filter just names the module. The full
file is
[`examples/envoy/dynamic-module.yaml`](https://github.com/huyz0/envoy-osproxy/tree/main/examples/envoy/dynamic-module.yaml).
The important part:

```yaml
http_filters:
  - name: envoy.filters.http.dynamic_modules
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter
      dynamic_module_config: { name: evoxy_module }
      filter_name: evoxy
      filter_config:
        "@type": type.googleapis.com/google.protobuf.StringValue
        value: |
          {
            "cluster": "opensearch",
            "partition_header": "x-tenant",
            "shared_index": "orders_shared",
            "inject_field": "_tenant"
          }
  - name: envoy.filters.http.router
```

The `filter_config` blob configures the reference tenancy. A custom tenancy reads
whatever configuration it needs from this same blob.

## One current limitation

The module does not apply a per-request cluster override yet, so a tenancy that
returns a different `cluster` per request will not route to different clusters.
Single-cluster placement, with per-tenant index selection and shared-index
isolation, is the supported path today.

## Verifying it

The end-to-end test loads the built image and drives real traffic through it,
including a shared-index multi-tenant round-trip:

```sh
cargo xtask module-image
cargo test -p evoxy-extproc --test e2e_module -- --ignored
```
