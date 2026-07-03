# Building the ext_proc backend

The ext_proc backend runs your logic as a small gRPC server that Envoy calls per
request. Envoy sends the request phases over gRPC, your server returns header and
body mutations, and Envoy forwards the mutated request to OpenSearch. You get
process isolation and an independent deploy, at the cost of one out-of-process hop.

The service is generic over your tenancy, so a custom `TenancySpi` works the same
as the built-in one.

## Build prerequisites

A stable Rust toolchain is all you need. ext_proc is pure Rust over `tonic`, so
unlike the dynamic module it needs no `libclang`, no `protoc`, no Envoy SDK, and no
glibc pinning: your server is an ordinary binary you run next to Envoy, and it
compiles in the normal workspace gate. TLS termination is Envoy's job, so the server
links no wire crypto either.

The one thing to install beyond Rust is a C compiler (`cc`/`gcc`), which a few
transitive crates build against. If you package the server in a container, any
standard Rust build image already has it.

## The server

An ext_proc server is a `tokio` binary that serves `evoxy_extproc::ExtProcService`
over `tonic`. Use `mimalloc` as the global allocator; its per-thread sharded heaps
cut allocator contention on the request path, the same choice osproxy's own server
makes.

`Cargo.toml`:

```toml
[package]
name = "my-extproc-server"
version = "0.1.0"
edition = "2021"

[dependencies]
evoxy-extproc = "..."     # this repo
evoxy-filter = "..."      # this repo
custom-tenancy = "..."    # your tenancy crate
osproxy-tenancy = "=1.0.2"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
tonic = "0.14"
mimalloc = "0.1"
```

`src/main.rs`:

```rust
use custom_tenancy::TieredTenancy;
use evoxy_extproc::{ExtProcService, ExternalProcessorServer};
use evoxy_filter::Filter;
use osproxy_tenancy::TenancyRouter;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tenancy = TieredTenancy {
        partition_header: "x-tenant".to_owned(),
        cluster: "opensearch".to_owned(),
        premium: ["acme".to_owned()].into_iter().collect(),
    };
    let service = ExtProcService::new(Filter::new(TenancyRouter::new(tenancy)));

    tonic::transport::Server::builder()
        .add_service(ExternalProcessorServer::new(service))
        .serve("0.0.0.0:50051".parse()?)
        .await?;
    Ok(())
}
```

Swap `TieredTenancy` for `evoxy_filter::ReferenceTenancy` if you want the built-in
tenancy with no custom code.

`ExtProcService` has a few options worth knowing:

- `.with_max_request_body_bytes(n)` caps the buffered request body. A larger body is
  refused with `413` before the brain runs, which bounds the per-request working set.
- `.with_admin_token(token)` enables the runtime directive plane behind a bearer
  token. Without it the plane fails closed with `403`.
- `.with_observe_config(&cfg)` sets the whole observe surface (the admin token and
  the initial decision-header state) at once.

### Why there is no filter_config here

The dynamic module is a library Envoy loads, so Envoy hands it a `filter_config`
blob at load time, and the module parses its tenancy and observe knobs (`admin_token`,
`emit_decision`) from that one blob. ext_proc is the opposite shape: Envoy's ext_proc
filter only names your gRPC endpoint, it never passes an application config. Your
server is a process you run, so it reads its own config however you like, which is
more flexible than a static blob baked into Envoy's bootstrap.

If you want parity with the module's blob, feed the same JSON to the server from an
env var:

```rust
let observe = evoxy_filter::ObserveConfig::from_json(&std::env::var("EVOXY_OBSERVE")?);
let service = ExtProcService::new(filter).with_observe_config(&observe);
// EVOXY_OBSERVE='{"admin_token":"change-me","emit_decision":true}'
```

`ObserveConfig::from_json` reads the same `admin_token` / `emit_decision` keys the
module reads, so one config shape works for both backends.

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

## Verifying it

The end-to-end test in the repository runs this backend against a real OpenSearch
behind Envoy:

```sh
cargo test -p evoxy-extproc --test e2e -- --ignored
```
