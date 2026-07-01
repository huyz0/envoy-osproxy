# ADR-004: Isolate the Envoy-ABI cdylib; keep the filter brain gated behind an abstraction

**Status:** Accepted

## Context

The dynamic module must bind Envoy's C ABI. The Rust SDK
(`envoy-dynamic-modules-rust-sdk`) does this via `bindgen`, which needs
`libclang` and Envoy's ABI headers at build time, and couples the artifact to a
specific Envoy build.

Our workspace is currently pure Rust: `cargo xtask ci` runs anywhere with just
the pinned toolchain. Depending on the SDK directly would force `libclang` + the
Envoy headers onto every gate run and every contributor, and pin the whole
workspace to an Envoy ABI version, a heavy, brittle imposition for a dependency
only the final cdylib needs.

osproxy hit the same shape with `librdkafka` and solved it by making
`osproxy-kafka-rdkafka` a **workspace-excluded** crate (built on its own, never
in the gate). We mirror that.

## Decision

Split the dynamic module into two crates:

- **`evoxy-filter`**, *gated, pure Rust.* The filter brain: it takes an
  [`evoxy_abi::FilterRequest`], runs the adapter → `evoxy-route` pipeline, and
  drives an **`EnvoyActions`** trait, our own SDK-agnostic abstraction of the
  Envoy filter effects we need (set upstream cluster, rewrite method/path/body,
  mutate headers, send a local reply). It also ships the reference tenancy for
  the default artifact. Fully unit-testable against a fake `EnvoyActions`; no SDK
  dependency, so it stays in the workspace and the gate.

- **`evoxy-module`**, *workspace-excluded.* The thin cdylib that depends on the
  Envoy SDK, implements the SDK's real HTTP-filter trait by adapting each callback
  to `EnvoyActions`, drives the async `evoxy-route` resolve on a captured runtime
  handle, and exposes the `register!` entry (ADR-003) that monomorphizes the brain
  for the user's tenancy type. Built only on a host with `libclang` + the SDK,
  documented in its README; excluded from `cargo xtask ci` exactly like osproxy's
  `osproxy-kafka-rdkafka`.

## Consequences

- The gate stays pure Rust and hermetic; the ABI-heavy, environment-specific
  build is quarantined to one excluded crate.
- The brain is decoupled from the SDK's exact API and Envoy version: an SDK bump
  or ABI change touches only `evoxy-module`'s thin adapter, never the tested
  logic. `EnvoyActions` is the stable seam.
- The brain is verifiable *now*, without Envoy: `evoxy-filter` tests assert that a
  `PreparedForward` yields the right `EnvoyActions` calls + `Continue`, and an
  `Immediate` yields a local reply + `Stop`.
- Async driving (Envoy filter callbacks are sync; `Router::resolve` is async) is
  an `evoxy-module` concern: it `block_on`s a current-thread runtime (the
  reference/in-memory placements resolve immediately) or buffers the body and
  resumes. The brain exposes an async `handle`; the driving strategy is not baked
  into it.
- `register!` and the SDK entry live in `evoxy-module` because the brain is
  generic over `R: Router` (whose `async fn resolve` is not `dyn`-compatible), so
  the concrete tenancy type must be monomorphized at the module boundary.
