# Building the dynamic module

The dynamic module runs your logic in-process, as a shared library that Envoy loads
through its dynamic-modules interface. There is no separate service and
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

So the prerequisites are: a stable Rust toolchain, `clang` and `libclang`, and (for
the image-baking step below) Docker. The `cargo xtask module-image` command builds
the `.so` inside a bookworm container, so it satisfies the glibc constraint for you;
Docker is the only host requirement for that path.

## The one command

The repository builds the module and bakes it into an Envoy image in one step:

```sh
cargo xtask module-image
```

This produces `evoxy-envoy:v1.37.0`: `envoyproxy/envoy:v1.37.0` with
`libevoxy_module.so` dropped into its dynamic-modules search path.

## Building your own module

You do not edit our source. Your module is its own small `cdylib` crate: depend on
`evoxy-module-sdk`, and call `register!` once with a factory that turns Envoy's
`filter_config` blob into your router. That is the whole `src/lib.rs`:

```rust
use custom_tenancy::TieredTenancy;
use osproxy_tenancy::TenancyRouter;

evoxy_module_sdk::register!(|config: &str| {
    // Parse whatever your tenancy needs from Envoy's filter_config blob.
    let _ = config;
    TenancyRouter::new(TieredTenancy {
        partition_header: "x-tenant".to_owned(),
        cluster: "opensearch".to_owned(),
        premium: ["acme".to_owned()].into_iter().collect(),
    })
});
```

The macro generates Envoy's module entry point and wires your factory in; the SDK
binding is generic over any tenancy, so the request and response transform work the
same. Your `Cargo.toml` is a `cdylib` with three dependencies:

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
evoxy-module-sdk = { git = "https://github.com/huyz0/envoy-osproxy", tag = "v0.1.0" }
custom-tenancy = { path = "../custom-tenancy" }  # your TenancySpi crate
osproxy-tenancy = "=1.0.2"

[profile.release]
panic = "abort"   # a module panic must not unwind into an Envoy worker
```

Then `cargo build --release` produces your `.so`. The Envoy SDK is pulled in
transitively, so your crate never names it, and no cargo feature flag is needed.

`evoxy-module-sdk` is a **git** dependency, not a crates.io one: it links the Envoy
dynamic-modules SDK, which lives in the `envoyproxy/envoy` git tree, and crates.io
forbids git dependencies at publish time. That never affects you, because you build
and deploy a `.so` and never publish it. A complete, compiling version is
[`examples/custom-module`](https://github.com/huyz0/envoy-osproxy/tree/main/examples/custom-module);
bake it into an Envoy image the same way the reference module does (the Dockerfile
just names a different `.so`).

The tenancy is compiled into the `.so`, not loaded at runtime, so rebuild after any
change.

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

## Per-request cluster routing

A tenancy that returns a different cluster per request routes to a different
upstream. The module sets the resolved cluster on the `x-evoxy-cluster` request
header at the header phase, and Envoy selects the upstream from header-matched
routes. Your bootstrap needs those routes, one per cluster plus a default:

```yaml
routes:
- match: { prefix: "/", headers: [{ name: x-evoxy-cluster, string_match: { exact: opensearch_b } }] }
  route: { cluster: opensearch_b }
- match: { prefix: "/", headers: [{ name: x-evoxy-cluster, string_match: { exact: opensearch_a } }] }
  route: { cluster: opensearch_a }
- match: { prefix: "/" }
  route: { cluster: opensearch_a }   # default
```

The built-in reference tenancy can drive this with a `cluster_by_partition` map in
its `filter_config` (`{"acme":"opensearch_a","globex":"opensearch_b"}`); a custom
tenancy just returns the cluster from `placement_for`.
[`examples/envoy/dynamic-module-multicluster.yaml`](https://github.com/huyz0/envoy-osproxy/tree/main/examples/envoy/dynamic-module-multicluster.yaml)
is a ready config, and the `per_tenant_cluster_routes_to_different_upstreams` live
test proves two tenants land in two different OpenSearch backends.

## Routing to endpoints without defining clusters

If you would rather not enumerate clusters at all (the way standalone osproxy just
returns an endpoint from the SPI and dials it), return the upstream endpoint from
`placement_for` (`.with_endpoint("http://os-eu.internal:9200")`). The module puts
its host on the request `:authority`, and Envoy's built-in `dynamic_forward_proxy`
resolves and dials it on demand, with no cluster defined for that upstream and no
control plane. Adding a tenant is then just returning its endpoint; nothing in the
Envoy config changes.

The reference tenancy drives this with an `endpoint_by_partition` map; the ready
config is
[`examples/envoy/dynamic-forward-proxy.yaml`](https://github.com/huyz0/envoy-osproxy/tree/main/examples/envoy/dynamic-forward-proxy.yaml),
and the `dynamic_forward_proxy_dials_the_tenancy_endpoint` live test proves two
tenants reach two OpenSearch backends that no cluster was defined for. The trade-off
versus named clusters is one shared upstream TLS/health/pool config for all dialed
hosts; for a fleet of OpenSearch clusters behind one CA that is usually fine. For a
changing set of clusters each with distinct upstream config, pair this with CDS.

### HTTPS upstreams (e.g. AWS ALBs)

HTTPS upstreams are the ideal case, not a limitation: return an `https://…` endpoint
and Envoy dials it over TLS. Because SNI and certificate-hostname validation follow
the per-request host (`auto_sni` + `auto_san_validation`), one cluster serves any
number of HTTPS hosts, and since ALBs all chain to public CAs a single trust store
validates every one. Return `https://host:443` (the module fills in `:443` for an
`https` URL with no port); point the cluster's `trusted_ca` at the image's CA bundle
(or your private CA). The ready config is
[`examples/envoy/dynamic-forward-proxy-tls.yaml`](https://github.com/huyz0/envoy-osproxy/tree/main/examples/envoy/dynamic-forward-proxy-tls.yaml).
This only falls short if each tenant's upstream uses a *distinct private* CA; then
give each its own cluster (static or via CDS/SDS).

## Verifying it

The end-to-end test loads the built image and drives real traffic through it,
including a shared-index multi-tenant round-trip:

```sh
cargo xtask module-image
cargo test -p evoxy-extproc --test e2e_module -- --ignored
```
