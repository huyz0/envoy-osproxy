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

## M1 — single-doc write path

Wire `evoxy-adapter` into an actual Envoy filter (dynamic module first, per
ADR-001) and drive `Pipeline::handle` for single-document ingest against a real
OpenSearch container. Reuse `osproxy-engine` + a reference tenancy/sink. Map
`PipelineResponse` → `FilterResponse`/upstream forward.

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
