# Examples

envoy-osproxy is a toolkit, not a turnkey proxy. There is no binary to run
directly. To put it in front of your OpenSearch you implement a tenancy (or use the
reference tenancy), build an artifact, and configure Envoy. This directory has a
compiling tenancy example ([`custom-tenancy/`](custom-tenancy)), a complete
custom-tenancy module cdylib ([`custom-module/`](custom-module)), a runnable
capture / async fan-out bridge ([`capture-bridge/`](capture-bridge)), and the Envoy
bootstraps ([`envoy/`](envoy)). The [user guide](https://huyz0.github.io/envoy-osproxy/)
covers each step in full.

## Implement the tenancy

Your tenancy implements `osproxy_spi::TenancySpi`, the same trait the standalone
osproxy uses. [`custom-tenancy/src/lib.rs`](custom-tenancy/src/lib.rs) is a real,
compiling example: `TieredTenancy` picks the physical index by tenant tier and uses
shared-index isolation. You implement `resolve_partition`, `doc_id_rule`,
`injected_fields`, and `placement_for`; the rest have defaults.

To try envoy-osproxy without writing code, skip this. The built-in
`ReferenceTenancy` partitions by a request header or the mTLS principal and does
dedicated-cluster or shared-index isolation, all from the Envoy `filter_config`
blob.

## Build an artifact

The dynamic module is generic over your tenancy. Your module is its own small
`cdylib` that depends on `evoxy-module-sdk` and calls `register!` once with a factory
that builds your router. [`custom-module/`](custom-module) is the complete crate; the
whole `src/lib.rs` is:

```rust
use custom_tenancy::TieredTenancy;
use osproxy_tenancy::TenancyRouter;

evoxy_module_sdk::register!(|config: &str| {
    let _ = config; // parse Envoy's filter_config blob for your knobs
    TenancyRouter::new(TieredTenancy {
        partition_header: "x-tenant".to_owned(),
        cluster: "opensearch".to_owned(),
        premium: ["acme".to_owned()].into_iter().collect(),
    })
});
```

`cargo build --release` in that crate produces your `.so`. `evoxy-module-sdk` is a
git dependency because it links the Envoy SDK, which crates.io forbids at publish
time. That is harmless here: you build and deploy a `.so`, you never publish it. To
bake the reference module into a stock Envoy image, run `cargo xtask module-image`.

The ext_proc server is a small `tokio` and `tonic` binary serving `ExtProcService`.
The reference-tenancy server the live tests run is a few lines:

```rust
use evoxy_extproc::{ExtProcService, ExternalProcessorServer};
use evoxy_filter::{Filter, ReferenceTenancy};
use osproxy_tenancy::TenancyRouter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tenancy = ReferenceTenancy::new("opensearch", "http://opensearch.internal:9200", "x-tenant");
    let service = ExtProcService::new(Filter::new(TenancyRouter::new(tenancy)));
    tonic::transport::Server::builder()
        .add_service(ExternalProcessorServer::new(service))
        .serve("0.0.0.0:50051".parse()?)
        .await?;
    Ok(())
}
```

Both backends are generic over your tenancy, so `TieredTenancy` (or any
`TenancySpi`) works on either. Swap `ReferenceTenancy` for your type in the code
above.

## Configure Envoy

Stock Envoy, no rebuild. Load the artifact and map the logical clusters your
placement returns to real OpenSearch upstreams:

- [`envoy/dynamic-module.yaml`](envoy/dynamic-module.yaml) loads the module filter.
- [`envoy/extproc.yaml`](envoy/extproc.yaml) points the ext_proc filter at your
  server.

Both are commented sketches. Fill in your OpenSearch addresses.

## What works today, and what doesn't

Verified end to end on both backends: partitioning by header or mTLS principal;
dedicated-cluster and shared-index isolation; the request transform (physical-index
path rewrite, field injection, partition-scoped id) and response reshaping;
`_bulk`/`_mget`/`_msearch`; bounded-memory caps; the migration write gate; and
per-request cluster routing (a tenancy that returns a different cluster per tenant
lands each tenant on a different upstream, via `x-evoxy-cluster` header-matched
routes; see [`envoy/dynamic-module-multicluster.yaml`](envoy/dynamic-module-multicluster.yaml)).
