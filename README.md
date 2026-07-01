# envoy-osproxy

A sister project of [`opensearch-proxy`](../opensearch-proxy) (osproxy). Same
capabilities — multi-tenant isolation, body reshaping, `_bulk` demux,
epoch-gated migration, shape-only observability with runtime directives, traffic
capture — but delivered as an **extension of a stock Envoy**, without forking or
recompiling Envoy.

**Why it's tractable:** osproxy already split the wire (`osproxy-transport`) from
the brain (`osproxy-engine::Pipeline::handle(RequestCtx) -> PipelineResponse`).
Envoy replaces the transport; the existing, tested engine crates become the
plug-in logic behind Envoy's `ext_proc` (out-of-process gRPC) seam, with a
dynamic-module (in-process Rust `.so`) fast path as a later drop-in.

## Status

Design phase. Start here:

- [docs/00-technical-analysis.md](docs/00-technical-analysis.md) — technical
  approach: the four no-rebuild extension mechanisms, capability mapping,
  boundary shifts (FIPS→Envoy-BoringSSL, tracing, admin plane), performance
  implications, and a milestone plan.

## Non-negotiable: no Envoy rebuild

Operators run an unmodified Envoy release. Our logic ships as bootstrap config
plus a loadable artifact (an `ext_proc` service container, and later a
`dynamic_modules` `.so`). We never patch Envoy source.
