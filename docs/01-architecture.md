# 01. Architecture

## The one idea

Envoy owns the wire; the reused osproxy engine is the brain; **one adapter crate
is the seam between them**. Nothing else is novel. This is a direct consequence of
osproxy already isolating its brain behind a plain data struct (`RequestCtx`) and
a single entry point (`Pipeline::handle`). See
[00-technical-analysis](00-technical-analysis.md) §2 for the derivation.

```
        client
          │  TLS/mTLS · HTTP/1.1|H2|gRPC  (Envoy terminates)
          ▼
   ┌──────────────┐   filter seam    ┌──────────────────────────────┐
   │  stock Envoy │◄────────────────►│ envoy-osproxy (Rust)          │
   │  (unmodified)│  FilterRequest ⇄ │  evoxy-abi   → wire model      │
   └──────┬───────┘  FilterResponse  │  evoxy-adapter → RequestCtx    │
          │ upstream cluster         │  osproxy tenancy+rewrite (reuse) │
          ▼                          │  admin plane: /_evoxy/*        │
     OpenSearch                      └──────────────────────────────┘
```

## Layers

- **`evoxy-abi`**: the Envoy-facing wire model. `FilterRequest` is what a filter
  receives (method, `:path`, authority, version, headers, body, Envoy-validated
  `MtlsIdentity`); `FilterResponse` is an immediate reply. Both extension
  mechanisms (§ below) decode into these *same* types. Pure leaf, no I/O, no
  osproxy dependency.

- **`evoxy-adapter`**: the seam. `RequestParts::from_filter` extracts owned
  facets once (classifying the path into an `EndpointKind`, deriving the principal
  from mTLS identity), and `RequestParts::ctx()` builds the borrowing
  `osproxy_spi::RequestCtx` the engine consumes. This is the whole port thesis in
  one function.

- **reused osproxy engine**: pulled from crates.io (`osproxy-tenancy`/`-rewrite`/
  `-spi`/`-core`, pinned `=1.0.2`), unchanged, not vendored. The adapter hands it a
  `RequestCtx`; `evoxy-route` resolves it through the tenancy `Router` and applies
  the body transform, producing either the mutated request Envoy forwards or a
  fail-closed `FilterResponse` (transform-then-forward, ADR-002). It never
  dispatches. Reuse grows per milestone ([roadmap](11-roadmap.md)); the
  transport/server crates are never reused.

## The extension seam: two backends, one adapter

The filter can plug into Envoy two ways (ADR-001), and both consume the same
`FilterRequest`, so the choice is a deployment knob:

- **`ext_proc`** (out-of-process gRPC): process isolation, independent scale,
  one localhost/UDS hop.
- **dynamic module** (in-process Rust `.so`, C ABI): no hop, osproxy's own latency
  profile; a panic crashes the Envoy worker, so INV-3 is mandatory.

## Boundaries that moved off us to Envoy

TLS/mTLS termination, HTTP/2, connection pooling, circuit breaking, retries, load
balancing, access logs, base tracing span, all Envoy's now. What stays ours:
tenancy/rewrite/migration/observability semantics, and the admin/introspection
plane (`/_evoxy/explain/<target>`, `/_evoxy/admin/directives`, `/_evoxy/metrics`),
which Envoy has no notion of and which the backend answers on reserved paths. FIPS for the wire
becomes Envoy-BoringSSL; our aws-lc-rs seam survives only for app-level HMAC. See
[00-technical-analysis](00-technical-analysis.md) §5.
