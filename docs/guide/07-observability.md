# Admin and observability

Envoy already gives you the wire-level telemetry: access logs, tracing spans,
retries and circuit-breaker stats, and its own admin `/stats`. What Envoy cannot see
is the tenancy decision: which partition a request resolved to, what transform ran,
whether a write was held for a migration. evoxy adds a thin layer for exactly that,
carried over from osproxy's observability model.

One rule governs all of it, inherited from osproxy: **the signals are shape-only.**
They carry kinds, flags, counts, and status codes, never a tenant value (no
partition id, index name, document id, or body). That is what makes them safe to
leave on, even the introspection surfaces.

## What you get

These are served on reserved paths, answered by the filter itself before any
routing, so they ride Envoy's own listener port with no second server. **Both
backends expose them** (the ext_proc service and the dynamic module run the same
brain), so `/_evoxy/metrics`, `/_evoxy/explain/...`, the decision header, and the
directive plane behave identically whichever you deploy.

### `GET /_evoxy/metrics`

A shape-only counter snapshot, meant to stay on in production. No auth, because it
leaks nothing:

```json
{ "requests": 12094, "routed": 12071, "rejected": 23 }
```

`routed` is requests forwarded upstream; `rejected` is fail-closed replies
(unresolved partition, isolation reject, stale-epoch, over-cap). Per instance by
design; a fleet rollup is your metrics system's job.

### The `x-evoxy-decision` response header

On every response, a shape-only summary of what the filter did, the "why did this
route here" Envoy cannot produce:

```
x-evoxy-decision: transform=inject;migration=settled;isolation=on;trace=4bf9…4736
```

`transform` is the body-transform kind (`none`/`inject`/`construct_id`/`both`),
`migration` the partition's phase, `isolation` whether a partition-scoping field was
injected. `trace` is the W3C trace-id when the request carried a `traceparent`, so
the decision correlates with Envoy's span. An operator can silence the header at
runtime through the directive plane below.

### `GET /_evoxy/explain/<target path>`

A routing dry-run: ask what a path *would* do without sending it. Shape-only, so it
is safe to expose:

```
GET /_evoxy/explain/orders/_search       (with the tenant header set)
→ {"endpoint":"search","outcome":"route","decision":"transform=inject;migration=settled;isolation=on"}

GET /_evoxy/explain/orders/_doc/1        (no tenant header)
→ {"endpoint":"get_by_id","outcome":"reject","status":400,"code":"partition_unresolved"}
```

This is the break-glass "why did this route here" for an operator, without touching
real data.

### `POST /_evoxy/admin/directives`, the runtime knob

The one surface that changes behavior, so it is **token-gated**. Today it carries one
directive: whether the decision header is emitted.

```
POST /_evoxy/admin/directives?emit_decision=false
Authorization: Bearer <token>
→ { "emit_decision": false }        # 200; the change is live, no restart
```

Without a configured token, or with a wrong one, it fails closed `403`. The plane is
off unless you deliberately enable it. The comparison is constant-time, so a wrong
token cannot be recovered by timing.

## Enabling it

`/_evoxy/metrics`, `/_evoxy/explain/...`, and the decision header need no
configuration; they are shape-only and always answered. Only the directive plane
needs a token (there is no default, so it is off until you provide one). How you set
it depends on the backend.

The **dynamic module** reads it from the `filter_config` blob, alongside the tenancy
config, so admin is config-only like everything else:

```json
{ "shared_index": "orders", "admin_token": "s3cret", "emit_decision": true }
```

The `admin_token` and `emit_decision` keys are the reserved observe schema; the
tenancy keys share the same blob and are ignored by the observe parser (and vice
versa). Omit `admin_token` and the plane stays fail-closed `403`.

The **ext_proc** backend has no `filter_config` blob (Envoy's ext_proc config only
points at your gRPC endpoint), so you set it in your small server binary, typically
from an env var:

```rust
use evoxy_extproc::{ExtProcService, ExternalProcessorServer};

let service = ExtProcService::new(filter)
    .with_admin_token(std::env::var("EVOXY_ADMIN_TOKEN")?);   // gate the directive plane
```

`ExtProcService::with_observe_config(&ObserveConfig::from_json(blob))` is also
available if you prefer to drive it from the same JSON shape as the module.

If you want to restrict even the always-on paths, match the `/_evoxy/` prefix in your
Envoy route config and gate it there. They are ordinary HTTP paths on the same
listener.

## Tracing

evoxy does not generate spans; Envoy does. What evoxy adds is correlation: it reads an
incoming `traceparent` and surfaces its trace-id on the decision header and in the
explain output, so a tenancy decision lines up with the Envoy span for the same
request. Request headers (including `traceparent`) pass through to the upstream
unchanged, so context propagates the way it would through any Envoy hop.

## Relationship to osproxy

This is a deliberate subset of osproxy's observability. osproxy also ships a
break-glass capture tape, an OTLP span exporter, a fleet directive store, and
structured request logs. Those are engine features that a standalone proxy owns
end to end; behind Envoy, the base telemetry (spans, logs, stats) is Envoy's job, so
evoxy exposes only the tenancy-decision layer on top: the metrics, the decision
header, the explain dry-run, and the one runtime directive. The shape-only,
fail-closed, no-value-leak posture is the same.
