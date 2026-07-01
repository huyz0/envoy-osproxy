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
local replies — no Envoy needed. **(1b-module) `evoxy-module` — scaffolded
(workspace-excluded, ADR-004).** Pinned SDK dep behind a `sdk` feature; the pure
driver (`Module`/`on_request`, `block_on`) compiles standalone; the SDK binding
(EnvoyActions-over-handle + `register!`) is the one host-gated seam (needs
libclang), documented in the crate README. Remaining 1b-module: write+verify that
SDK seam on a build host.

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

**(1c) next:** an `#[ignore]` testcontainer integration test — real OpenSearch +
stock Envoy (ext_proc filter → our service) — asserting a doc written through
Envoy lands and round-trips. Runnable *here* (Docker is available), unlike the
dynamic-module path.

User-facing SPI model is settled in ADR-003 and shown in
[06-wiring-example](06-wiring-example.md): the user implements the same
`osproxy-spi` traits, statically compiled into a `cdylib` via `register!`, and
drops the `Sink` seam (Envoy forwards).

## M2 — read path

get-by-id, delete-by-id, `_search`, `_count`. Verify write→read symmetry (the
logical-id round-trip) end-to-end through Envoy.

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
