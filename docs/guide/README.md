# Introduction

Multi-tenant OpenSearch proxy capabilities delivered as an extension of a stock
Envoy, without forking, patching, or recompiling Envoy. Point a standard
`envoyproxy/envoy` release at your OpenSearch cluster, load one artifact, and get
per-tenant isolation, request and response reshaping, `_bulk`/`_mget`/`_msearch`
demux, epoch-gated migration, shape-only observability, and async fan-out.

## An extension of osproxy that leverages Envoy

envoy-osproxy is an extension of [osproxy](https://github.com/huyz0/opensearch-proxy),
the standalone multi-tenant OpenSearch proxy. It runs osproxy's logic inside Envoy
instead of osproxy's own HTTP server.

osproxy already separates its brain from its wire. The brain is a
transport-agnostic engine that turns a request into a routing and transform
decision. The wire is the server that speaks HTTP. This project keeps the brain
unchanged and replaces the wire with Envoy:

- osproxy provides the multi-tenant logic: tenancy and placement, isolation,
  request and response reshaping, `_bulk`/`_mget`/`_msearch`, epoch-gated migration.
- Envoy provides the wire: HTTP/1.1, HTTP/2 and gRPC, TLS and mTLS, connection
  pooling, load balancing, retries, and circuit breaking.
- envoy-osproxy is the layer between them. It hands each Envoy request to the
  osproxy engine and applies the decision back onto Envoy.

The engine crates come from crates.io, so a `cargo build` resolves everything and
there is no other repository to check out.

## This is a toolkit, not a ready-to-run proxy

There is no binary to `docker run`. To put envoy-osproxy in front of OpenSearch you
do three things:

1. Implement the tenancy, or use the built-in reference tenancy for a no-code start.
2. Build an artifact: an out-of-process ext_proc gRPC server, or an in-process
   dynamic-module `.so`.
3. Configure Envoy: load the artifact and map your logical clusters to real
   OpenSearch upstreams.

Start with [Implementing a tenancy](02-tenancy.md), then build a backend
([ext_proc](03-build-extproc.md) or [dynamic module](04-build-module.md)).

## Two backends, one brain

The same logic runs behind either Envoy extension point. This is a deployment
choice, not a rewrite. Pick the dynamic module when latency is the priority and you
can accept a shared crash domain; pick ext_proc when process isolation and an
independent deploy matter more than a couple of milliseconds.

| backend | mechanism | measured added latency | trade-off |
|---|---|---|---|
| ext_proc | out-of-process gRPC sidecar | +2.3 ms over Envoy | process isolation, independent deploy |
| dynamic module | in-process Rust `.so` | about 0 ms over Envoy (within the noise) | lowest latency, shared crash domain |

Both are verified end to end through a stock, unmodified Envoy. The
[backend comparison](05-backends.md) has the measured numbers.

## Where to go next

- [Architecture](01-architecture.md) covers the components and how a request flows.
- [Implementing a tenancy](02-tenancy.md) is the code you write.
- [Building the ext_proc backend](03-build-extproc.md) and
  [Building the dynamic module](04-build-module.md) are the two deployment paths.
- [ext_proc vs. dynamic module](05-backends.md) helps you pick one.
- [Benchmarks](06-benchmarks.md) has the measured latency, concurrency, and
  transform-cost numbers.
- [Admin and observability](07-observability.md) covers the shape-only metrics,
  decision header, explain dry-run, and the runtime directive plane.
