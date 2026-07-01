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

**(M2b) response-side reshaping — done and proven live.** `shape_get_response`/
`shape_search_response` return a document/hit in the client's logical view
(`_index`/`_id` logical via `map_physical_to_logical`, injected fields stripped
from `_source`); `shape_read_response` resolves + dispatches. Wired onto the ext_proc
**response path** (`response_body_mode: BUFFERED`; the response phase rebuilds the
request from the buffered headers and reshapes the body; `content-length` dropped
so Envoy recomputes). `ReferenceTenancy` gained a **shared-index mode** (config
`shared_index`): inject the isolation field + partition-scoped id.

**Multi-tenant isolation proven end-to-end (`shared_index_isolates_tenants`):**
two tenants (`acme`, `globex`) write the same natural key to one shared physical
index through stock Envoy; each reads back and searches seeing **only its own
document**, in its logical view (logical id, `_tenant` stripped) — inject-on-write,
partition-scoped id, query partition-filter, and strip/unmap-on-read, all live.

**(M2c multi-cluster e2e)** — prove cluster selection via the dynamic module's
header-phase routing (uses the `resolve_cluster`/`route_headers` primitives), or a
resolved ext_proc routing mode.

## M3 — `_bulk` / `_mget` / `_msearch`

**(M3a) `_bulk` request rewrite — done and proven live.** `evoxy-route::bulk`
parses the NDJSON (`parse_bulk`) and rewrites each item in place: the action
line's `_index` → physical index and `_id` → partition-scoped physical id, and
each source line has the isolation fields injected (reusing `transform::apply`
per line); forwarded as one bulk to the cluster-level `/_bulk`. A unit test
asserts the rewritten NDJSON, and the shared-index e2e now **bulk-writes through
Envoy** and confirms the two extra docs land isolated with logical ids. (Single
upstream, as ADR-002; cross-cluster bulk demux would need fan-out, out of scope.)

**(M3b) `_bulk` response reshaping — done and proven live.**
`evoxy-route::response::shape_bulk_response` walks the `_bulk` response `items[]`
(each a one-key object keyed by the verb) and returns each result in the client's
logical view: logical `_index`, and the physical `_id` mapped back to logical via
`map_physical_to_logical` (best-effort — an irreversible template leaves the id
as-is). Wired into `shape_read_response` for `IngestBulk` (so the ext_proc
response phase already drives it; `content-length` dropped so Envoy recomputes). A
unit test asserts the reshaped `items[]`, and the shared-index e2e now asserts the
bulk **response** carries logical ids (`10`, `11`) and the logical index — not the
partition-scoped physical ids.

**(M3c) `_mget`/`_msearch` demux — done and proven live.** `evoxy-route::demux`
rewrites both multi-operation reads for the single upstream (ADR-002):
`rewrite_mget` pins every fetch to the physical index with a partition-scoped id
(`parse_mget`, reusing `read::physical_id`); `rewrite_msearch` forces every header
line's index to the physical index and wraps each query with the mandatory
partition filter (`parse_msearch` + `read::filtered_query`) — a client naming
another index in a header cannot escape its placement. Responses are mapped back
to the logical view: `shape_mget_response` (per `docs[]` entry) and
`shape_msearch_response` (per `responses[]` search), reusing the per-hit reshape.
Wired into `prepare`/`shape_read_response`; the shared-index e2e now runs `_mget`
(ids `1`,`10`) and a two-query `_msearch` **through Envoy**, asserting logical ids,
`_tenant` stripped, and tenant isolation. (Single upstream; cross-cluster fan-out
is out of scope.)

**(M3d) bounded-memory request bodies — done and proven live.** The transform-
then-forward model must hold the whole body to rewrite it (parse the NDJSON,
splice the fields, construct ids), so an unbounded body is an unbounded
allocation. The ext_proc service now enforces a **request-body cap**
(`ExtProcService::with_max_request_body_bytes`, default
`DEFAULT_MAX_REQUEST_BODY_BYTES` = 32 MiB): a body over the cap is refused with a
fail-closed **`413` `payload_too_large`** (shape-only body) *before* the brain
buffers or transforms it. Two unit tests (over-cap → 413, at-cap boundary
allowed) and the `write_then_read` e2e now sends an oversized body through stock
Envoy and asserts the `413`.

**Finding — true chunked streaming rewrite is `FULL_DUPLEX_STREAMED`, deferred.**
Envoy's plain `STREAMED` body mode forwards each chunk as the processor returns
it, so it cannot rewrite a *whole* body (the last chunk's mutation only replaces
the last chunk). The correct mechanism for buffer-then-rewrite-then-stream with
bounded memory is `FULL_DUPLEX_STREAMED` (Envoy ≥1.34, `BodySendMode` = 4), which
also requires trailer mode `SEND` and `StreamedBodyResponse` chunks. It carries a
real constraint for our model: the `:path`/`:method` mutations a write derives
from the body must ride the **header** response, which precedes the body — so the
service must buffer headers+body+trailers before emitting any response (allowed by
the mode) to keep fail-closed correctness (an isolation reject discovered from the
body must still be an `ImmediateResponse`, not a request already committed
upstream). This is the next increment; the cap already bounds memory today. It is
also where the ext_proc-vs-module cost of body handling is measured (docs/00 §6).

## M4 — Envoy-owned TLS/mTLS

Principal from Envoy-validated identity (XFCC/SAN) rather than self-parsed certs;
delete any residual transport concerns. mTLS-for-mutation policy expressed in
Envoy + adapter.

**(M4a) principal from XFCC — done.** Envoy terminates mTLS and forwards the
validated client identity in the `x-forwarded-client-cert` (XFCC) header;
`MtlsIdentity::from_xfcc` (in `evoxy-abi`) parses it — quote-aware, reads only the
**peer** element of the chain, takes its `Subject` DN and `URI` SANs — and the
ext_proc `convert::filter_request` populates the request identity from it (else
the default, not-presented). The principal the brain keys tenancy on
(`stable_id`: first URI SAN, else Subject) is therefore Envoy-validated, never
self-parsed. The filter trusts the header because Envoy owns it
(`forward_client_cert_details: SANITIZE_SET`); the filter never sees a raw
certificate. Six `xfcc` unit tests (SPIFFE URI SAN, quoted-Subject-with-commas,
chain peer-only, subject-only fallback, empty, malformed) + two `convert` tests.

**(M4b) mTLS-for-mutation policy + live mTLS proof — done.** The filter gained
`Filter::with_require_mtls_for_mutation`: a write (`EndpointKind::is_write`) with
no presented client identity fails closed with `403`
`mtls_required_for_mutation`, before routing (reads are unaffected); three brain
unit tests. The reference tenancy gained `partition_from_principal`: it resolves
the partition from the Envoy-validated principal (`stable_id`) instead of a client
header — so the tenant cannot be spoofed by a request header.

The live proof (`tests/mtls.rs`, `#[ignore]`'d) generates a CA + server + client
cert at runtime (`rcgen`), stands up **stock Envoy with a downstream mTLS
listener** (`require_client_certificate`, `SANITIZE_SET`, URI/subject XFCC), and a
client presenting the acme cert writes **with no `x-tenant` header at all**. The
whole M4 chain runs end to end: Envoy validates the cert → sets XFCC → `convert`
parses the identity → the mutation policy admits the write → the partition is the
cert principal. The decisive assertion queries OpenSearch **directly** and finds
the stored physical doc's `_tenant` is `CN=acme` (the Envoy-validated principal),
partition-scoped id `CN=acme:1` — the identity, not a header, drove tenancy.

**Finding — a doc id derived from a slash-bearing principal (a SPIFFE URI) needs
path percent-encoding.** The proof uses the cert Subject DN (`CN=acme`, a valid
path segment); a `spiffe://td/acme` principal would build
`/idx/_doc/spiffe://td/acme:1`, whose slashes OpenSearch rejects (`no handler
found`). Percent-encoding the doc-id path segment on write and by-id read is a
small routing hardening, tracked as a follow-up (the id is unchanged in `_bulk`/
`_mget`/`_msearch`, which carry it in JSON, not the path).

## M5 — migration + async fan-out

**(M5a) migration write gate — done and proven live.** `prepare` now runs the
write gate on **every** write path (`EndpointKind::is_write` — ingest, `_bulk`,
delete): after resolving, it calls `Router::admit_write(partition, epoch)`; a
partition in the cutover window is **held** with a fail-closed, retryable `409`
`stale_epoch` (reads are never gated — they always resolve to a single placement).
This is in-model: the write is rejected, never dispatched. The `ReferenceTenancy`
gained `with_migration(partition, phase)` (reusing `osproxy_spi::MigrationPhase`):
a `Cutover` partition's `admit_write` returns false and its placement carries the
phase for observability; a real fleet reads this from a `MigrationStore`. Three
route unit tests (write→409, read allowed, `_bulk`→409); the dedicated e2e marks a
`frozen` tenant in cutover and asserts a write through stock Envoy is held `409`
while a read is not gated.

**(M5b) async fan-out — designed (ADR-005), not built.** An Envoy HTTP extension
cannot cleanly produce to Kafka: stock Envoy has no HTTP→Kafka routing, the
dynamic module has no runtime to produce from, and putting a producer in the
ext_proc service reintroduces the dispatch/delivery concerns ADR-002 shed.
[ADR-005](decisions/005-async-fanout-via-mirror.md) decides fan-out is expressed
as an Envoy `request_mirror_policies` (shadow) to a dedicated **HTTP→Kafka bridge**
cluster — Envoy mirrors, a purpose-built bridge (reusing osproxy's async-write
seam) produces, the filter stays pure. The broker bridge + a live Kafka-mirror
e2e are deferred; **no Kafka producer is added to the extension or the service.**

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
