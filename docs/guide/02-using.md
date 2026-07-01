# Using envoy-osproxy

envoy-osproxy is a **toolkit, not a turnkey proxy** — there is no ready-to-run
binary. A deployment does three things. Runnable, compiling versions of everything
below live in the
[examples directory](https://github.com/huyz0/envoy-osproxy/tree/main/examples).

## 1. Implement the tenancy

Your placement and isolation logic is a small Rust type implementing the tenancy
trait — the *same* trait the standalone osproxy uses. You implement four methods:
which partition a request belongs to, whether it uses a partition-scoped document
id, which fields to inject for isolation, and where the partition is placed.
[`examples/custom-tenancy`](https://github.com/huyz0/envoy-osproxy/tree/main/examples/custom-tenancy)
is a real, compiling example (it places each tenant into a per-tier shared index).

You do **not** write a `main`, an upstream client, or any TLS — Envoy is the app
and forwards upstream for you.

**Just trying it out?** Skip this step. The built-in **reference tenancy**
(partition by request header or mTLS principal; dedicated-cluster or shared-index
isolation) is configured entirely from Envoy's filter config — no code.

## 2. Build an artifact

Pick a backend (see [ext_proc vs. dynamic module](03-backends.md)):

### Dynamic module — in-process, supports a custom tenancy today

The module is generic over your tenancy, so a custom implementation works
end-to-end. The module crate is your **build template**: wire your tenancy into its
factory and build the image, which drops your `.so` into a stock Envoy:

```sh
cargo xtask module-image
```

### ext_proc — out-of-process, isolated

An ext_proc server is a small async binary that serves the processing service over
gRPC. The reference-tenancy server (what the live tests run) is a few lines:

```rust
use evoxy_extproc::{ExtProcService, ExternalProcessorServer};
use evoxy_filter::{Filter, ReferenceTenancy};
use osproxy_tenancy::TenancyRouter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tenancy = ReferenceTenancy::new("opensearch", "http://opensearch:9200", "x-tenant");
    let service = ExtProcService::new(Filter::new(TenancyRouter::new(tenancy)));
    tonic::transport::Server::builder()
        .add_service(ExternalProcessorServer::new(service))
        .serve("0.0.0.0:50051".parse()?)
        .await?;
    Ok(())
}
```

> **Current limitation:** the ext_proc service is built for the reference tenancy
> today. For a *custom* tenancy, prefer the dynamic module (above); making ext_proc
> generic over any tenancy is a known next step.

## 3. Configure Envoy

Stock Envoy, no rebuild. Load the artifact and map the logical clusters your
placement returns to real OpenSearch upstreams. Complete example bootstraps:

- [`envoy/dynamic-module.yaml`](https://github.com/huyz0/envoy-osproxy/tree/main/examples/envoy/dynamic-module.yaml)
- [`envoy/extproc.yaml`](https://github.com/huyz0/envoy-osproxy/tree/main/examples/envoy/extproc.yaml)

## What works today, and what doesn't

- **Verified end-to-end** (both backends): partition by header or mTLS principal;
  dedicated-cluster and shared-index isolation; request transform (physical-index
  path rewrite, field injection, partition-scoped id) and response reshaping;
  `_bulk`/`_mget`/`_msearch`; bounded-memory caps; the migration write gate.
- **Not yet honored live:** per-request cluster override in the module (so
  multi-cluster placement does not yet route to different clusters — single-cluster
  is the supported path), and a custom tenancy on the ext_proc backend.
