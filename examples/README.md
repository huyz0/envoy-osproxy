# Examples — how to actually run this

**envoy-osproxy is a toolkit, not a turnkey proxy.** There is no binary to
`docker run`. To put it in front of your OpenSearch you do three things:

1. **Implement the tenancy SPI** (or use the built-in reference tenancy).
2. **Build an artifact** — an ext_proc gRPC server, or a dynamic-module `.so`.
3. **Configure Envoy** — load the artifact and map logical clusters to upstreams.

This directory has a compiling SPI example ([`custom-tenancy/`](custom-tenancy))
and two Envoy bootstraps ([`envoy/`](envoy)).

---

## 1. Implement the SPI

Your tenancy is an `osproxy_spi::TenancySpi` — the *same* trait the standalone
osproxy uses. [`custom-tenancy/src/lib.rs`](custom-tenancy/src/lib.rs) is a real,
compiling example (`TieredTenancy`: picks the physical index by tenant tier, with
shared-index isolation). You implement `resolve_partition`, `doc_id_rule`,
`injected_fields`, and `placement_for`; the rest have sensible defaults.

**Just trying it out?** Skip this — the built-in `ReferenceTenancy` (header- or
mTLS-principal partitioning, dedicated-cluster or shared-index isolation) is
configured entirely from the Envoy `filter_config` blob, no code.

## 2. Build an artifact

### Dynamic module (in-process, lowest latency) — supports a custom SPI today

`evoxy-module` is generic over your router (`Module<R>`), so a custom `TenancySpi`
works end-to-end. The module crate is your **build template**: wire your tenancy
into the factory in [`crates/evoxy-module/src/sdk.rs`](../crates/evoxy-module/src/sdk.rs):

```rust
// in new_http_filter_config_fn, build YOUR tenancy instead of the reference one:
use custom_tenancy::TieredTenancy;
use osproxy_tenancy::TenancyRouter;

let tenancy = TieredTenancy {
    partition_header: "x-tenant".to_owned(),
    cluster: "opensearch".to_owned(),
    premium: ["acme".to_owned()].into_iter().collect(),
};
let module = Module::new(TenancyRouter::new(tenancy), runtime.handle().clone());
```

Then build the image: `cargo xtask module-image` (stock Envoy + your `.so`).

### ext_proc (out-of-process, isolated) — reference tenancy today

An ext_proc server is a small `tokio`/`tonic` binary serving `ExtProcService`.
The reference-tenancy server (what the live tests run) is:

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

> **Current limitation (honest):** `ExtProcService` is monomorphized on
> `ReferenceTenancy` today (a `Send`-bound limitation of async-fn-in-trait for a
> generic router — see the note in `crates/evoxy-extproc/src/service.rs`). For a
> *custom* SPI, prefer the dynamic module above; making ext_proc generic over any
> `TenancySpi` is a known next step.

## 3. Configure Envoy

Stock Envoy, no rebuild. Load the artifact and map the logical `ClusterId`s your
placement returns to real OpenSearch upstreams:

- [`envoy/dynamic-module.yaml`](envoy/dynamic-module.yaml) — the `DynamicModuleFilter`.
- [`envoy/extproc.yaml`](envoy/extproc.yaml) — the `ext_proc` filter + your server.

Both are commented sketches; fill in your OpenSearch addresses.

---

## What works today, and what doesn't

- **Verified end-to-end** (live tests, both backends): header/principal
  partitioning; dedicated-cluster and shared-index isolation; request transform
  (physical-index path rewrite, field injection, partition-scoped id) and response
  reshaping; `_bulk`/`_mget`/`_msearch`; bounded-memory caps; migration write gate.
- **Not yet honored live:** per-request **cluster override** (`set_upstream_cluster`
  is a no-op in the module today), so multi-cluster placement decisions do not yet
  route to different clusters — single-cluster placement is the supported path. A
  **custom SPI on ext_proc** needs the generic-router work noted above.
