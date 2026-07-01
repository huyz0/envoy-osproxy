# 09. Testing and Quality

Two-tier quality, inherited from osproxy: **deterministic gates** (this repo's
`cargo xtask ci`) plus **LLM semantic review** (the quality-review agent in
[`.claude/agents/`](../.claude/agents/quality-reviewer.md)) for what the gates
cannot judge, altitude, cohesion, naming, and whether tests/docs are meaningful.

## Determinism

Behavior must not depend on wall-clock time or unseeded randomness read directly:
`Instant::now`/`SystemTime::now` are banned by `clippy.toml`'s
`disallowed-methods`. Code that needs time takes an injected clock (reusing
`osproxy_core::time::Clock` once the engine is wired at M1). This keeps tests
reproducible and the microbenchmarks stable.

## Test kinds

- **Unit tests**, colocated in each crate. `evoxy-abi` covers the wire model
  (path/query split, case-insensitive headers, gRPC detection, identity
  precedence); `evoxy-adapter` covers classification (every `EndpointKind`) and
  ctx construction (principal, protocol, method, query, body fidelity).
- **Doctests**, the adapter's public example builds a `RequestCtx` and asserts
  its facets, so the README-level usage is compiler-checked.
- **Microbenchmarks**, `crates/evoxy-adapter/benches/adapt.rs`, iai-callgrind
  instruction counts (deterministic, diffable in CI), covering the classifier and
  the full seam. Run via `cargo xtask bench`.
- **Integration (M1+)**, against a real OpenSearch container behind a real Envoy,
  reusing osproxy's testcontainer patterns and `osproxy-bench` for NFR-P.

## Traceability

Each request carries Envoy's `x-request-id` into the `RequestCtx` as its
`RequestId`, so a request is followable across the Envoy hop into the engine's
shape-only `/debug/explain` (wired at M7). Telemetry stays shape-only and
read-only (inherited invariant).

## AI-assisted debugging (the point of the observability design)

As in osproxy, the target is that an external AI agent can *diagnose blind* from
captured shape-only signals: `/debug/explain` (per-request decision chain),
`/debug/breakglass` (bounded tape), `/metrics`, and structured logs, all served
on our port, since Envoy has no notion of them. The blind-diagnosis exit test
(engine-side) is reused; M7 re-asserts it end-to-end through Envoy.
