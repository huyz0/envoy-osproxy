# Introduction

Multi-tenant OpenSearch proxy capabilities delivered as an extension of a stock
Envoy — without forking, patching, or recompiling Envoy. Point a standard
`envoyproxy/envoy` release at your OpenSearch cluster, load one artifact, and get
per-tenant isolation, request/response reshaping, `_bulk`/`_mget`/`_msearch` demux,
epoch-gated migration, shape-only observability, and async fan-out.

## An extension of osproxy that leverages Envoy

envoy-osproxy is an **extension of [osproxy](https://github.com/huyz0/opensearch-proxy)**
— the standalone multi-tenant OpenSearch proxy — that runs its logic inside Envoy
instead of osproxy's own HTTP server. osproxy already separates its *brain* (a
transport-agnostic engine that turns a request into a routing/transform decision)
from its *wire* (the server that speaks HTTP). This project keeps the brain
verbatim and swaps the wire for **Envoy**:

- **osproxy provides** the multi-tenant logic — tenancy and placement, isolation,
  request/response reshaping, `_bulk`/`_mget`/`_msearch`, epoch-gated migration.
- **Envoy provides** the wire — HTTP/1.1, HTTP/2 and gRPC, TLS and mTLS,
  connection pooling, load balancing, retries, and circuit breaking.
- **envoy-osproxy is the thin layer between them** — it hands each Envoy request to
  the osproxy engine and applies the decision back onto Envoy.

The engine crates come from crates.io, so a `cargo build` resolves everything and
there is no other repository to check out.

## Not turnkey

This is a **toolkit, not a ready-to-run proxy.** To put it in front of OpenSearch
you do three things:

1. **Implement the tenancy logic** — or use the built-in reference tenancy for a
   no-code start.
2. **Build an artifact** — an out-of-process ext_proc gRPC server, or an in-process
   dynamic-module `.so`.
3. **Configure Envoy** — load the artifact and map your logical clusters to real
   OpenSearch upstreams.

The [Using envoy-osproxy](02-using.md) guide walks all three with runnable code and
example Envoy configs.

## Two backends, one brain

The same logic runs behind either Envoy extension point — a deployment choice, not
a rewrite. See [ext_proc vs. dynamic module](03-backends.md) for the
benchmark-grounded comparison.

| backend | mechanism | measured added latency | trade-off |
|---|---|---|---|
| **ext_proc** | out-of-process gRPC sidecar | **+2.3 ms** over Envoy | process isolation, independent deploy |
| **dynamic module** | in-process Rust `.so` | **≈ 0 ms** over Envoy (in the noise) | lowest latency, shared crash domain |

Both are verified end-to-end through a stock, unmodified Envoy.

## Where to go next

- [Architecture](01-architecture.md) — the components and how a request flows.
- [Using envoy-osproxy](02-using.md) — implement, build, deploy.
- [ext_proc vs. dynamic module](03-backends.md) — pick a backend.
