# Introduction

envoy-osproxy turns a plain Envoy into a multi-tenant OpenSearch proxy. You point an
`envoyproxy/envoy` release at your OpenSearch cluster, load one artifact, and every
request gets per-tenant isolation, request and response reshaping,
`_bulk`/`_mget`/`_msearch` fan-out, epoch-gated migration, shape-only observability,
traffic capture, and async writes. Envoy keeps doing what it is good at (TLS, HTTP
codecs, pooling, load balancing, retries); the tenancy logic rides on top as an
extension, not a fork.

## Built on osproxy

The tenancy logic is not new here. It comes from
[osproxy](https://github.com/huyz0/opensearch-proxy), a standalone multi-tenant
OpenSearch proxy, whose one deliberate design choice makes this project possible:
osproxy separates its brain from its wire. The brain is a transport-agnostic engine
that turns a request into a routing and transform decision. The wire is whatever
speaks HTTP to move bytes. osproxy ships its own HTTP server as the wire;
envoy-osproxy keeps the brain byte-for-byte and swaps Envoy in as the wire. The
engine crates come straight from crates.io, so there is no second repository to check
out.

What that brain does is decide, per tenant, *where a request goes and how it is
reshaped*. osproxy offers three isolation models, and the one you pick is the main
knob:

- **Dedicated cluster**: each tenant gets its own OpenSearch cluster. The proxy just
  routes; nothing in the request body changes. Strongest isolation, most
  infrastructure.
- **Dedicated index**: tenants share a cluster but each gets its own physical index.
  The proxy rewrites the index in the request path (`/orders` becomes
  `/orders-acme`). A middle ground.
- **Shared index**: all tenants live in one physical index, kept apart by a
  partition field the proxy injects into each document and a partition-scoped
  document id it constructs, with reads filtered back to the caller's own data.
  Densest, cheapest, and the transform does the most work.

On top of placement sit a few per-request behaviors that are also "modes": some
indices can be marked passthrough and skip tenancy entirely; a write can be mirrored
for capture or fanned out to Kafka; and a write can run in async mode, where the
client gets an immediate `202` and the durable write happens off the request path.
Migration between placements is epoch-gated, so a tenant can move clusters or indices
without a stop-the-world cutover.

## Two ways to run it inside Envoy

The same brain runs behind either of Envoy's two extension points. This is a
deployment choice, not a rewrite, and the trade is straightforward.

| backend | mechanism | added latency | trade-off |
|---|---|---|---|
| dynamic module | in-process Rust `.so` | about 0 ms over Envoy | lowest latency, shared crash domain |
| ext_proc | out-of-process gRPC sidecar | a couple of ms | process isolation, independent deploy |

Pick the **dynamic module** when latency is the priority and you can accept that a
bug in the module shares Envoy's process. Pick **ext_proc** when you want the tenancy
logic in its own process, deployable on its own schedule, and a couple of
milliseconds is a fair price. Everything below, isolation, capture, async, and the
observability surfaces, works the same on both. The
[backend comparison](05-backends.md) walks through the choice.

## Most setups need no code

If the three isolation models cover you, you write no Rust at all. The built-in
reference tenancy is driven entirely by an Envoy `filter_config` blob: pick the
isolation model, say where the tenant comes from (a header, the mTLS principal, or a
path segment), and map tenants to clusters or index names. Capture, async writes, and
the admin/observability surfaces are configured the same way. The
[configuration-only guide](08-config-only.md) is the whole no-code path, and
[capture and async fan-out](09-capture-and-fanout.md) covers the traffic-shaping
modes.

## When config is not enough: a tenancy SPI

Some tenancies need real logic: a physical index that mixes the tenant with the
client's index name, placement looked up from a store, a tenant derived from a JWT
claim, or a live migration state machine. For those you implement `TenancySpi`, the
same Rust trait osproxy uses. It is a small trait, and the reference tenancy is a
working example to copy from. Start at
[implementing a tenancy](02-tenancy.md), then build one of the two artifacts:
[the ext_proc server](03-build-extproc.md) or
[the dynamic module](04-build-module.md). Both are generic over your tenancy, so the
code you write plugs into either backend unchanged.

## What it costs

The overhead is small and measured, not guessed. The dynamic module adds no latency
you can distinguish from Envoy's own noise; ext_proc adds a couple of milliseconds
for its out-of-process hop. The per-request transform work, even the shared-index
path that rewrites the body, is microseconds, swamped by OpenSearch's own write
latency. Throughput scales with concurrency instead of inflating latency: in the
harness, going from 1 to 32 concurrent clients raised throughput about 20x (55 to
1,100 rps) while the median stayed roughly flat (18 to 25 ms). Every number comes from a repo harness you can re-run; the
[benchmarks page](06-benchmarks.md) has the setup and the tables.

## Where to go next

- [Architecture](01-architecture.md) covers the components and how a request flows.
- [Benchmarks](06-benchmarks.md) has the measured latency, concurrency, and
  transform-cost numbers.
- [Configuration-only mode](08-config-only.md) is the no-code path: drive the
  reference tenancy from `filter_config`.
- [Capture and async fan-out](09-capture-and-fanout.md) covers the traffic-capture
  and async-write modes.
- [Implementing a tenancy](02-tenancy.md) is the code you write when config is not
  enough.
- [Building the ext_proc backend](03-build-extproc.md) and
  [Building the dynamic module](04-build-module.md) are the two deployment paths.
- [ext_proc vs. dynamic module](05-backends.md) helps you pick one.
- [Admin and observability](07-observability.md) covers the shape-only metrics,
  decision header, explain dry-run, and the runtime directive plane.
