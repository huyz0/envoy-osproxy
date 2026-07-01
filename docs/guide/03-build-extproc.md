# Building the ext_proc backend

The ext_proc backend runs your logic as a small gRPC server that Envoy calls per
request. Envoy sends the request phases over gRPC, your server returns header and
body mutations, and Envoy forwards the mutated request to OpenSearch. You get
process isolation and an independent deploy, at the cost of one out-of-process hop.

## The server

An ext_proc server is a `tokio` binary that serves `evoxy_extproc::ExtProcService`
over `tonic`. This is the same service the live tests run.

`Cargo.toml`:

```toml
[package]
name = "my-extproc-server"
version = "0.1.0"
edition = "2021"

[dependencies]
evoxy-extproc = "..."     # this repo
evoxy-filter = "..."      # this repo
osproxy-tenancy = "=1.0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
tonic = "0.14"
```

`src/main.rs`:

```rust
use evoxy_extproc::{ExtProcService, ExternalProcessorServer};
use evoxy_filter::{Filter, ReferenceTenancy};
use osproxy_tenancy::TenancyRouter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Partition by the `x-tenant` header, dedicated cluster "opensearch".
    let tenancy = ReferenceTenancy::new("opensearch", "http://opensearch:9200", "x-tenant");
    let service = ExtProcService::new(Filter::new(TenancyRouter::new(tenancy)));

    tonic::transport::Server::builder()
        .add_service(ExternalProcessorServer::new(service))
        .serve("0.0.0.0:50051".parse()?)
        .await?;
    Ok(())
}
```

`ExtProcService` has two options worth knowing:

- `.with_max_request_body_bytes(n)` caps the buffered request body. A larger body is
  refused with `413` before the brain runs, which bounds the per-request working set.
- `.with_admin_token(token)` enables the runtime directive plane behind a bearer
  token. Without it the plane fails closed with `403`.

Run it like any binary. Package it in a container and deploy it next to Envoy.

## Configuring Envoy

Add the `ext_proc` HTTP filter pointing at your server's cluster, and route to
OpenSearch. The complete file is
[`examples/envoy/extproc.yaml`](https://github.com/huyz0/envoy-osproxy/tree/main/examples/envoy/extproc.yaml).
The important parts:

```yaml
http_filters:
  - name: envoy.filters.http.ext_proc
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
      grpc_service: { envoy_grpc: { cluster_name: my_extproc } }
      mutation_rules: { allow_all_routing: true, allow_envoy: true }
      processing_mode:
        request_header_mode: SEND
        request_body_mode: BUFFERED
        response_header_mode: SEND
        response_body_mode: BUFFERED
  - name: envoy.filters.http.router
```

The `mutation_rules` line lets the server rewrite routing and headers. The
`BUFFERED` body modes let it read and reshape whole bodies, which the isolation
transform needs.

## Custom tenancy on ext_proc

Today `ExtProcService` is built for the reference tenancy. The service type is fixed
to that tenancy because the router's async methods are not provably `Send` for a
generic type, and the gRPC response stream must be `Send`. For a custom tenancy
right now, use the [dynamic module](04-build-module.md), which is generic over the
tenancy. Making ext_proc generic over any tenancy is a known next step.

## Verifying it

The end-to-end test in the repository runs exactly this backend against a real
OpenSearch behind a stock Envoy:

```sh
cargo test -p evoxy-extproc --test e2e -- --ignored
```
