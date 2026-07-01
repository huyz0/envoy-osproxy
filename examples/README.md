# Examples

envoy-osproxy is a toolkit, not a turnkey proxy. There is no binary to run
directly. To put it in front of your OpenSearch you implement a tenancy (or use the
reference tenancy), build an artifact, and configure Envoy. This directory has a
compiling tenancy example ([`custom-tenancy/`](custom-tenancy)) and two Envoy
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

The dynamic module is generic over your tenancy, so a custom `TenancySpi` works end
to end. The module crate is your build template: build your tenancy in the factory
in [`crates/evoxy-module/src/sdk.rs`](../crates/evoxy-module/src/sdk.rs), then run
`cargo xtask module-image` to produce a stock Envoy image with your `.so`.

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

The ext_proc service is built for the reference tenancy today. The router's async
methods are not provably `Send` for a generic type, and the gRPC response stream
must be `Send`, so a custom tenancy uses the dynamic module for now. Making ext_proc
generic over any tenancy is a known next step.

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
`_bulk`/`_mget`/`_msearch`; bounded-memory caps; and the migration write gate.

Not honored live yet: a per-request cluster override, because the module does not
apply one, so a multi-tenant placement that varies the cluster per request does not
route to different clusters. Single-cluster placement is the supported path. A
custom tenancy on the ext_proc backend needs the generic-router work above.
