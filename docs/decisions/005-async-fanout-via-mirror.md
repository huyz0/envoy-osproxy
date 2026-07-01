# ADR-005: Async fan-out is Envoy request-mirroring to a bridge, not an in-filter Kafka produce

**Status:** Accepted

## Context

osproxy's async-write / capture arc (its own ADR-010) fans a write out to Kafka
using an in-process `krafka` producer: the proxy owns the wire, so it can dispatch
the request upstream *and* produce a copy to a broker. That is natural for a
standalone proxy.

The Envoy port does not own the wire (ADR-002): the filter **transforms and lets
Envoy forward**, it never dispatches. So "fan the write out to Kafka" has to be
re-examined — an Envoy HTTP extension is for *mutating the request/response* and
*participating in the routing decision*, and neither of those is "originate a
side-channel produce to a broker."

Concretely, can an Envoy extension produce to Kafka?

- **Envoy routing / the extension's routing participation** — no. Stock Envoy has
  no HTTP→Kafka produce path. Its Kafka support (`kafka_broker`, `kafka_mesh`) are
  *network* filters that proxy the Kafka wire protocol between Kafka clients and
  brokers; they do not accept an HTTP request and turn it into a produce.
- **The dynamic module** (in-process, on an Envoy worker thread) — effectively no.
  It has no async runtime, and a blocking produce would stall the worker. Smuggling
  in a background producer thread fights the SDK model and the crash-blast-radius
  posture (a producer panic takes Envoy down).
- **The ext_proc service** (a separate process with its own runtime) — technically
  yes: it *could* hold a `krafka` producer and produce as a side effect. But that
  reintroduces exactly the dispatch/delivery/backpressure/ordering concerns ADR-002
  shed, inside a service whose whole point is to be a pure transform brain. It also
  splits the "who owns delivery" story between Envoy (the forward) and us (the
  produce).

## Decision

**Async fan-out is expressed as an Envoy `request_mirror_policies` (shadow) to a
dedicated HTTP→Kafka bridge cluster — not an in-filter/in-service Kafka produce.**

- Envoy **mirrors** the (already transformed) request to a second upstream cluster,
  fire-and-forget, response ignored — the Envoy-native fan-out primitive.
- That cluster is a small **bridge service** that accepts the mirrored HTTP request
  and produces to Kafka. The bridge can reuse osproxy's async-write seam
  (`AckProducer` / the `krafka` producer) as a standalone binary; it owns delivery,
  backpressure, and ordering — the concerns that belong to a producer, kept out of
  the routing path.
- The **extension** stays pure: it participates in routing (it can select/annotate
  whether the mirror applies, e.g. per `X-Write-Mode`), and it transforms the body
  — but it does not talk to a broker.

The engine's async-write *contract* (honest 202 + `op_id`, refuse-don't-lie) is a
property of the **bridge**, not the filter; the filter's only fan-out role is
marking the request mirror-eligible.

## Consequences

- The fan-out delivery guarantees live in one purpose-built service, not smeared
  across Envoy + the ext_proc brain. The brain keeps the ADR-002 "never dispatch"
  invariant intact — no broker client links into the default filter/service.
- The bridge is a normal Envoy upstream: it gets Envoy's pooling, retries, and
  circuit-breaking on the mirror leg for free, same as the primary.
- `request_mirror_policies` is fire-and-forget by design, which matches async
  semantics: the client's response comes from the primary upstream; the mirror is
  best-effort. A durability-critical path would instead make the bridge the
  *primary* upstream (synchronous), which is a routing choice, not a filter change.
- **Scope for this milestone (M5):** the migration write-gate is delivered and
  proven live (a cutover write is held with a retryable `409`, in-model). The
  mirror-to-bridge fan-out is designed here; no Kafka producer is added to the
  extension or the ext_proc service.
- **Mechanism now proven live** (`tests/mirror.rs`, `#[ignore]`'d): a write flows
  through stock Envoy + our filter into OpenSearch (the primary), and Envoy's
  `request_mirror_policies` shadows the request — **as the filter transformed it**
  (physical index `orders_shared`, partition-scoped percent-encoded id
  `acme%3A1`, injected `_tenant`) — to a second cluster, a recording bridge. So the
  Envoy-native fan-out carries the *physical* request, fire-and-forget, with the
  filter staying pure. The recording bridge stands in for the HTTP→Kafka producer;
  the produce itself (osproxy's `krafka`/async-write seam) is the only remaining
  piece, and it is ordinary producer code, not novel.
- If a future need requires the produce to be synchronous/transactional with the
  write (true exactly-once across OpenSearch + Kafka), that is a different problem
  than fan-out and would get its own ADR.
