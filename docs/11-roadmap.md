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

**Finding, now resolved — a doc id derived from a slash-bearing principal (a
SPIFFE URI) needs path percent-encoding.** A `spiffe://td/acme` principal builds
the physical id `spiffe://td/acme:1`, whose slashes, left raw, split the `:path`
and OpenSearch rejects it (`no handler found`). `evoxy-route::encode` now
percent-encodes the doc-id segment and `_routing` value on the write and by-id
paths; OpenSearch decodes them back to the exact id, so the stored id and every
response are unchanged (`_bulk`/`_mget`/`_msearch` carry the id in JSON, not the
path, and are untouched). The mTLS e2e now uses a real SPIFFE **URI SAN** and
asserts the stored `_tenant`/id are `spiffe://td/acme` end to end.

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

## M6 — FIPS — **done**

The FIPS obligation for the **wire** leaves our code entirely: Envoy owns TLS
(ADR-002), so a FIPS-validated Envoy (BoringSSL-FIPS build) satisfies it, and our
extension links **no** data-path crypto. [ADR-006](decisions/006-fips-boundary.md)
records the boundary shift from osproxy's ADR-004 (which put an AWS-LC-FIPS module
*inside* its binary): here the wire is Envoy's, the Envoy↔ext_proc hop is a UDS
with no crypto in the sidecar model, and any future app-HMAC (M7 directive/cursor
tokens) reuses osproxy's `CryptoProvider` seam as an opt-in path.

Enforced, not just asserted: **`cargo xtask crypto-free`** (in `ci`) proves every
shipped `evoxy-*` crate's non-dev dependency tree contains no
`rustls`/`ring`/`aws-lc-*`/`openssl`/`boring`/`native-tls`. It passes today
(`tonic` without its `tls` feature); a stray crypto pull-in now fails the gate, so
adding wire TLS to the extension becomes a deliberate, reviewed act. The heavy
part of osproxy's M6 (suite pinning, a runtime FIPS-engaged check, a validated
module in-binary) does not port — Envoy carries it.

## M7 — observability + NFR-P

**(M7a) NFR-P A/B latency — done and measured live.** `tests/perf.rs`
(`#[ignore]`'d) times the *same* GET-by-id directly against OpenSearch (baseline)
and **through stock Envoy + our ext_proc filter** (proxy), both hitting the same
physical document so the difference is pure overhead. It reuses osproxy's pure
bench crate — `LatencySummary` → `NfrProfile` → `judge` → `Verdict` — and prints
the profile+verdict JSON as the operator/LLM substrate. Assertions are
host-independent (every request functional, the profile well-formed); absolute
latency is recorded, not gated.

**Measured here** (dev box, concurrency 1, 100 samples): baseline p50 ≈ 1.2 ms →
proxy p50 ≈ 4.3 ms, so **added p50 ≈ 3.0 ms / added p99 ≈ 4.3 ms**. That is the
cost of the **ext_proc IPC hop** — two gRPC round-trips (request + response
phases, both buffered) — and it quantifies the ext_proc-vs-module tradeoff
(docs/00 §6): a latency-sensitive deployment picks the in-process dynamic module
(no hop), an isolation-sensitive one accepts the hop. The `judge` fails the
*provisional in-process* NFR-P1 bound (2 ms), honestly — the ext_proc hop is not
an in-proc path; the JSON records the real numbers rather than pretending.

**(M7b) shape-only decision observability — done and proven live.** The extension
knows *why* a request routed where it did — the transform kind, migration phase,
and whether isolation applied — which Envoy cannot see. `evoxy-route::decision_shape`
renders it as a **shape-only** string (kinds and flags only: `transform=both;
migration=settled;isolation=on` — no partition, index, or id value, honoring the
no-value-leak rule), and the ext_proc response-headers phase surfaces it as an
`x-evoxy-decision` response header (`Filter::decision_shape`). Two route unit tests
(shared→`both`/isolation on with no value leak, dedicated→`none`/off); the
shared-index e2e asserts the header rides a real response through stock Envoy.

**(M7c) shape-only `/metrics` introspection — done and proven live.** The one
introspection surface meant to stay on in production. Rather than a second server,
the filter answers a **reserved path** (`/_evoxy/metrics`) with an immediate
response — so it rides Envoy's own port, fully in-model (an `ImmediateResponse`, no
dispatch). `Metrics` holds per-instance relaxed atomics (routed vs. fail-closed);
`finalize` tallies each data-plane outcome; a GET to the reserved path returns a
shape-only snapshot (`{"requests":N,"routed":N,"rejected":N}` — counts only, no
tenant value) short-circuited before routing (and not itself counted). Two metrics
unit tests + a `process_message` test (counts move, reserved path answered `200`);
the shared-index e2e reads `/metrics` **through stock Envoy** and asserts the
counters moved and total = routed + rejected. Per-instance by design — a fleet
rollup is an external aggregator's job.

**(M7d) shape-only `/explain` dry-run — done and proven live.** The break-glass
"why would this route here", served the same reserved-path way. A GET to
`/_evoxy/explain/<target path>` makes the filter resolve `<target path>` as
[`prepare`] would and return a **shape-only** JSON verdict — the endpoint kind, the
outcome (`route`/`reject`), and either the decision shape or the fail-closed
status/code — **without forwarding**. `evoxy-route::explain` shares `prepare`'s
supported-endpoint guard, resolution, and write-gate, so the explain can never
disagree with the real route. Two route unit tests (route with a shape-only
decision + no value leak; unresolved → reject 400) + a `/_evoxy/explain/...` live
e2e assertion through stock Envoy (acme's search explained as `route`; a missing
tenant as a `reject`).

**(M7e) trace-context reconciliation — done and proven live.** Envoy owns tracing:
it generates and propagates the W3C `traceparent` and forwards it upstream, so the
extension does not manage the span — it only **reads** the trace-id to correlate
its shape-only signals with Envoy's span. `evoxy_abi::trace_id_of` parses the
`traceparent` (`version-traceid-spanid-flags`, rejecting a malformed or all-zero
id); the decision header gains a `;trace=<id>` suffix and `/explain` a `"trace"`
field when present. A trace-id is a random token (not a tenant value), safe to
surface; `traceparent` is never mutated or stripped. Two abi parser tests + a
route and a filter test; the shared-index e2e sends a `traceparent` through stock
Envoy and asserts `/explain` echoes the trace-id.

**(M7f) directive plane — done and proven live.** The "act" half of
observe-then-act. A per-instance runtime `Directives` store (relaxed atomics, same
posture as `/metrics`) carries a shape-only behavior toggle (today: whether the
decision header is emitted). The token-gated `/_evoxy/admin/directives` reserved
path applies settings from the query (`?emit_decision=false`) and returns the
current snapshot; it fails closed `403` without a matching `Authorization: Bearer`
(constant-time compare, no crypto crate — the extension stays crypto-free,
ADR-006) and is off entirely unless a token is configured
(`ExtProcService::with_admin_token`). The filter reads the directive live to gate
the decision header. Three directive unit tests + a token-gated `process_message`
test; the shared-index e2e flips the directive **through stock Envoy** (403 without
the token, `200` with it) and confirms the next read no longer carries the decision
header — behavior changed **with no restart**. It is a *behavior* toggle, never a
security policy (those are set at deploy time, not over the wire).

**M7 is complete:** the extension exposes a coherent, in-model observability +
control plane on Envoy's own port — `/metrics`, `/explain`, the `x-evoxy-decision`
header, trace-context correlation, the NFR-P A/B substrate, and the token-gated
directive plane — all shape-only, all proven live through stock Envoy.

## v2 — the other backend

Whichever of {dynamic module, ext_proc} was not chosen first, added behind the
same adapter as a deployment option.
