# 11 — Roadmap

Milestones mirror osproxy's M1→M7 discipline, but each is *thinner* because the
engine is reused, not rebuilt. Each milestone lands with tests, a green
`cargo xtask ci`, and doc/ADR updates in the same commits.

## M0 — walking skeleton — **done**

The seam, proven in isolation. `evoxy-abi` (Envoy wire model) + `evoxy-adapter`
(`FilterRequest` → `RequestCtx`) with path/method classification, mTLS-derived
principal, unit tests, a doctest, and an iai-callgrind microbenchmark. Full gate,
hooks, spec-driven docs, and the quality-review agent are in place.

- **Exit criterion met:** given any Envoy request, we build the exact
  `RequestCtx` the standalone proxy builds (proven by `evoxy-adapter` tests).

## M1 — single-doc write path — *in progress*

Wire `evoxy-adapter` into an actual **Rust dynamic module** (ADR-001) using the
**transform-then-forward** model (ADR-002): per request, build the `RequestCtx`,
call the routing SPI for a `RouteDecision`, apply the `BodyTransform` via
`osproxy-rewrite`, rewrite the `:path` to `target.index` (+ construct `_id`),
select the Envoy upstream cluster from `target.cluster`, and return `Continue` so
Envoy forwards. Fail-closed cases (unknown endpoint, isolation reject, stale
epoch) become immediate filter responses.

Verification: a testcontainer harness stands up **real OpenSearch behind a stock
Envoy** loading our `.so`, and asserts a single document written through Envoy is
routed/transformed and round-trips. Reuse a reference tenancy/routing impl.

Sub-steps: **(1a) `evoxy-route` — done.** Reuses `Router::resolve` + the
`osproxy-rewrite` byte-splice primitives to turn a `RequestCtx` into a
`PreparedForward` (cluster + physical path + id + mutated body) or a fail-closed
response; never dispatches (ADR-002). 5 tests trace the `ROUTE-*` contract
([route-contract spec](specs/route-contract.md)), covering dedicated passthrough,
shared-index inject+construct-id, and the fail-closed paths. **(1b-brain) `evoxy-filter` — done.**
The filter brain, SDK-agnostic (ADR-004): `Filter::handle` runs adapter → route
and issues effects through an `EnvoyActions` abstraction (`ContinueUpstream` /
`StoppedWithLocalReply`), plus the `ReferenceTenancy` default and `FilterConfig`.
4 fake-`EnvoyActions` tests assert write-mutation+continue and the fail-closed
local replies — no Envoy needed. **(1b-module) `evoxy-module` — done
(workspace-excluded, ADR-004).** The pure driver (`Module`/`on_request`) compiles
standalone; the SDK binding (`src/sdk.rs`, `--features sdk`) implements the SDK's
`HttpFilter`/`HttpFilterInstance` over the brain via an owned `SdkActions` recorder
and `init!` registration. **Verified building `libevoxy_module.so`** — exports the
full `envoy_dynamic_module_event_*` ABI (a loadable module). SDK-0.1.x limits
routing/header rewrites to the header phase (M2), like ext_proc; the reference
default (static route) needs only body + fail-closed reply, which it does. Remaining:
an e2e through a real Envoy loading the `.so` (parallels the ext_proc e2e).

**(1b-extproc) `evoxy-extproc` — done (the verifiable-here backend, ADR-001).** An
Envoy External Processing gRPC service (`tonic` + `envoy-types`, pure Rust, no
libclang) over the *same* `evoxy-filter` brain: it assembles a `FilterRequest`
from the ext_proc header/body phases, runs the brain via an `EnvoyActions` that
records ext_proc mutations, and streams back a `ProcessingResponse` that rewrites
`:method`/`:path`, sets the `x-evoxy-cluster` routing header (+`clear_route_cache`),
and replaces the body — or an `ImmediateResponse` (fail-closed). 3 tests drive
`process_message` directly (headers-phase continue, body-phase route+body
mutation, unresolved-partition → 400). The tonic service is concrete over the
reference tenancy (a generic service can't spawn: `Router::resolve`'s `async fn`
future isn't provably `Send` generically — a user-tenancy service is the same
shape monomorphized, deferred).

**(1c) done — the thesis proven end-to-end.** `tests/e2e.rs` (`#[ignore]`,
`--ignored`) stands up **real OpenSearch + stock Envoy** (ext_proc filter → our
`ExtProcService` served in-process), writes `PUT /orders/_doc/42` *through Envoy*,
and reads it straight back from OpenSearch — asserting the document (`k`, `who`)
landed. No Envoy rebuild; stock `envoyproxy/envoy` image + a bootstrap + our Rust.
Both upstreams reached via the host gateway (`host.docker.internal`, `V4_ONLY`).
M1 routes statically to the single reference cluster; header-based multi-cluster
selection needs header-phase re-routing (a body-phase header mutation does not
reliably re-route) and lands with M2.

**M1 is complete** (write path, both backends: gated ext_proc verified live +
excluded dynamic-module scaffold).

User-facing SPI model is settled in ADR-003 and shown in
[06-wiring-example](06-wiring-example.md): the user implements the same
`osproxy-spi` traits, statically compiled into a `cdylib` via `register!`, and
drops the `Sink` seam (Envoy forwards).

## M2 — read path — *in progress*

**(M2a) request-side read routing — done.** `evoxy-route` now handles `GetById`,
`DeleteById`, `Search`, and `Count`: it maps the client's logical id to the
physical id (`SharedIndex` constructs a partition-scoped id via
`map_logical_to_physical`; dedicated keeps the client id), routes to the physical
index, and injects the **mandatory partition filter** into search/count queries
(`wrap_query`) — the read isolation boundary (ADR-006). The `ROUTE-*` spec and 5
new `evoxy-route` tests cover dedicated passthrough, `SharedIndex` id-map, and the
search filter. The e2e now also **reads the document back through Envoy**
(verified live), so write→read round-trips end to end.

**(M2c) header-phase routing — foundation done; live ext_proc re-routing blocked.**
`evoxy-route::resolve_cluster` and `evoxy-filter::route_headers` resolve the
upstream cluster from the request headers (before the body) and set it — the
header-phase routing primitives, unit-tested. **Finding:** wiring this into the
live ext_proc path does **not** re-route: with `request_body_mode: BUFFERED`,
Envoy commits the upstream (`pool ready`) *before* applying the ext_proc header
response, so `x-evoxy-cluster` + `clear_route_cache` arrive too late (a header-
phase clear before a body phase even 504s). This is an Envoy ext_proc timing
nuance, not a defect in the routing logic. The primitives stay as the foundation:
the **dynamic module** sets headers directly on the map at its `request_headers`
callback (no such race), and a future ext_proc mode (e.g. header-only routing, or
`request_body_mode` tuning) can use them. The live e2e keeps the proven static
single-cluster route.

Remaining M2: **(M2b) response-side reshaping** — strip injected fields from
`_source` and map physical ids back to logical, on Envoy's response path. **(M2c
multi-cluster e2e)** — prove cluster selection via the dynamic module's
header-phase routing, or a resolved ext_proc routing mode.

## M3 — `_bulk` / `_mget` / `_msearch`

Body-mutating endpoints with Envoy's **STREAMED** body mode to preserve osproxy's
bounded-memory NDJSON demux. This is where the ext_proc-vs-module cost of body
handling is measured (docs/00 §6).

## M4 — Envoy-owned TLS/mTLS

Principal from Envoy-validated identity (XFCC/SAN) rather than self-parsed certs;
delete any residual transport concerns. mTLS-for-mutation policy expressed in
Envoy + adapter.

## M5 — migration + async fan-out

Epoch-gated write gate and async write mode, reusing `osproxy-tenancy::migration`
and the async-write seam.

## M6 — FIPS

Adopt Envoy-BoringSSL-FIPS for the wire; keep the app-level HMAC seam. Document
the boundary shift from osproxy's ADR-004.

## M7 — observability + NFR-P

Admin/introspection plane on our port; reconcile tracing with Envoy's span; reuse
`osproxy-bench` (`NfrProfile`/`judge`) for the proxy-vs-baseline verdict, now
measuring Envoy + our filter against direct OpenSearch.

## v2 — the other backend

Whichever of {dynamic module, ext_proc} was not chosen first, added behind the
same adapter as a deployment option.
