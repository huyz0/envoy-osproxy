# ADR-001: Extend a stock Envoy via a Rust filter, never a fork

**Status:** Accepted

## Context

We want the osproxy capability set in front of OpenSearch, but built *on* Envoy
so we inherit its production transport (TLS, HTTP/2, pooling, LB, circuit
breaking) instead of hand-building it again. The hard constraint from the project
brief: **do not rebuild Envoy** — operators run a stock release.

osproxy's engine is already transport-agnostic (`Pipeline::handle(&RequestCtx)`),
so the question is purely *which Envoy extension seam* hosts it, and in *what
language*.

## Options

1. **`ext_proc`** — an out-of-process gRPC service Envoy streams headers/body to.
   Language-free, full process isolation, pure bootstrap config, zero rebuild.
   Cost: one localhost/UDS hop + (de)serialization per request.
2. **Dynamic modules** — a shared library Envoy `dlopen`s over a stable C ABI
   (Envoy ≥ 1.34). In-process, no hop, osproxy's own latency profile. The only
   first-class SDK is **Rust**. Cost: a panic/UB crashes the Envoy worker; the ABI
   evolves across Envoy minors.
3. **proxy-wasm** — in-process WASM sandbox. Rejected: the sandbox constrains
   crypto/FIPS and adds body-copy overhead on our body-reshaping hot path.
4. **ext_authz / Lua** — allow/deny + header mutation only; cannot rewrite bodies
   or demux `_bulk`. Adjunct at best, not sufficient.

## Decision

Extend a **stock Envoy** with a **Rust filter**, behind a single `RequestCtx`
adapter (`evoxy-adapter`) so the backend is a deployment knob, not a rewrite:

- **Dynamic module (Rust) is the primary** when latency matters. We track *latest*
  Envoy, so the ABI-churn caveat is moot, and Rust gives zero-language-boundary
  reuse of the osproxy engine crates.
- **`ext_proc` is co-equal** when process isolation / independent scaling matters;
  same adapter, decode into the same `FilterRequest`.

We **never fork or recompile Envoy.** Ship bootstrap config plus a loadable
artifact (an `ext_proc` container and/or a `dynamic_modules` `.so`).

## Consequences

- A dynamic-module panic is an Envoy-worker crash, so the panic-free lint posture
  (`deny(unwrap_used, expect_used, panic)`) inherited from osproxy is now a hard
  safety requirement, not hygiene (INV-3).
- We inherit Envoy's transport and **delete** most of osproxy's transport/server
  surface; the reused crates are the engine brain only.
- FIPS for the wire becomes Envoy-BoringSSL-FIPS; our aws-lc-rs seam survives only
  for app-level HMAC (directive/cursor tokens).
- The admin/introspection plane stays on our own port — Envoy has no notion of it.
- Language for the module: **Rust** (the only maintained dynamic-modules SDK, and
  what lets us link the engine crates directly).
