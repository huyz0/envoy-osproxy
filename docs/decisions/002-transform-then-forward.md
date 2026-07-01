# ADR-002: The filter transforms and lets Envoy forward (no in-filter dispatch)

**Status:** Accepted

## Context

osproxy's `Pipeline::handle` owns the whole request lifecycle including
**dispatch**: its `Sink`/`Reader` seams make the upstream HTTP call to
OpenSearch, using osproxy's own pooled connections. That is correct for a
standalone proxy that owns the wire.

In the Envoy port, Envoy owns the wire — including upstream connection pooling,
load balancing, health checking, circuit breaking, and retries (the entire
reason to adopt Envoy, ADR-001). If our filter also dispatched, we would bypass
all of that and throw away the benefit.

The engine already separates the two concerns cleanly. `RoutingSpi::route(ctx)`
yields a pure [`RouteDecision`] — a single `Target` (cluster + index), a
`BodyTransform`, `HeaderOp`s, and the placement `Epoch` — with **no dispatch**.
`osproxy-rewrite` applies the `BodyTransform` as a byte-splice. Dispatch is a
separate downstream step in `Pipeline::handle`, not part of deriving the
decision.

## Decision

The Envoy filter runs the engine's **transform** stage and then **returns
control to Envoy to forward**. Concretely, per request the filter:

1. builds the `RequestCtx` (via `evoxy-adapter`);
2. calls the routing SPI to get a `RouteDecision`;
3. applies `body_transform` (via `osproxy-rewrite`) to the body, rewrites the
   `:path` from the logical index to `target.index` (constructing `_id` for
   id-templated writes), and applies `header_ops`;
4. selects the Envoy **upstream cluster** from `target.cluster` (via the
   dynamic-module routing API / cluster metadata);
5. returns `Continue` so **Envoy forwards** the mutated request with its own
   connection pool.

The filter only produces an **immediate response** itself for fail-closed cases
(isolation rejection, unknown endpoint, stale-epoch write) — never for the happy
path.

We do **not** use `Pipeline::handle`'s dispatch path in-filter. The reused engine
surface is the routing SPI + `osproxy-rewrite` + the plan/transform helpers, not
the `Sink`.

## Consequences

- Envoy's pooling/LB/circuit-breaking/retries apply to every upstream call for
  free; osproxy's own pool telemetry and scalability work are subsumed (docs/00
  §6). One less thing for us to build or defend.
- The `Target → Envoy cluster` mapping becomes a first-class config surface: each
  logical `ClusterId` must correspond to an Envoy cluster in the bootstrap. This
  is the seam where placement meets Envoy routing.
- Read-path and `_bulk`/`_mget`/`_msearch` (M2/M3) that need **response**
  transformation will use Envoy's response path (`on_response_body`), still
  transform-not-dispatch: mutate the streamed response, never re-issue it.
- Migration write-gating (M5) keeps the engine's epoch stamping; a stale-epoch /
  cutover decision becomes an immediate `409` from the filter (fail-closed),
  matching osproxy's live `StaleEpoch → 409`. Async **fan-out** cannot be an
  in-filter dispatch either (an extension can't produce to Kafka) — it is Envoy
  request-mirroring to a bridge, [ADR-005](005-async-fanout-via-mirror.md).
- If a future need genuinely requires in-filter dispatch (e.g. scatter that Envoy
  cannot express), it would be a new ADR superseding this one — but v1 single-target
  routing (inherited ADR-002 of osproxy) never does.
