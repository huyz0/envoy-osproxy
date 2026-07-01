# ADR-003: SPI is compiled in statically; the user owns a cdylib/service artifact

**Status:** Accepted

## Context

In osproxy the user implements the `osproxy-spi` traits and **compiles them in
statically** — no runtime plugins (osproxy ADR-007). The osproxy binary *is* the
user's SPI plus the engine. osproxy also ships a reference tenancy and a runnable
binary, so it is usable out of the box and as a library.

In the Envoy port, Envoy is the pre-built app; the user does not write a `main`
and does not build Envoy. So: **how does the user's SPI get into the request
path, given Envoy provides the app and the upstream handling?**

Two facts shape the answer:

1. Envoy owns forwarding/pooling (ADR-002), so the user no longer implements the
   `Sink`/`Reader` **dispatch** seam — only the tenancy/routing/rules *brain*.
2. The extension seam is a Rust artifact Envoy loads (a dynamic-module `cdylib`)
   or calls (an `ext_proc` service `bin`) — ADR-001.

## Decision

**The SPI programming model is unchanged; only the artifact shape changes.** The
user implements the same `osproxy-spi` traits in a Rust crate, statically
compiled into the extension artifact. We support both ownership models, mirroring
osproxy:

- **Primary — user owns the artifact (library model).** We ship `evoxy-filter` as
  a library exposing a `register!` macro that takes the user's SPI factory
  `Fn(FilterConfig) -> impl TenancySpi`. The user's crate is
  `crate-type = ["cdylib"]` (dynamic module) or a `bin` (ext_proc), depends on
  `evoxy-filter` + `osproxy-spi`, and calls `register!`. `cargo build --release`
  yields the `.so`/binary that Envoy loads/calls. This preserves ADR-007's
  compile-in-static contract and gives maximum flexibility.
- **Also — a default runnable artifact.** We build a default cdylib that links a
  **reference tenancy**, so a newcomer gets a working `.so` out of the box (the
  mirror of osproxy shipping `ReferenceTenancy` + a runnable binary).

Configuration reaches the SPI through **Envoy's filter config** (the
`dynamic_modules` config blob) plus environment, read once at module init and
passed to the user's factory. For ext_proc, ordinary config file/env.

The `Sink`/`Reader` seams are **not** implemented by the user here — Envoy
forwards (ADR-002).

## Reconciling with ADR-007 (no dynamic plugins)

The `.so` is dynamic **only at the Envoy↔module boundary**, which is Envoy's own
loading mechanism. The user's SPI is **statically linked into** that `.so`; there
is no plugin runtime *inside* our module, no WASM/dylib SPI loading. Changing SPI
logic means rebuilding the artifact and restarting Envoy. The dynamism ADR-007
forbids — runtime-swappable SPI plugins — still does not exist. ADR-007 holds.

## Consequences

- The user's deliverable shrinks to a tenancy/routing crate + a one-line
  `register!`; no `main`, no transport, no dispatch. Simpler than osproxy.
- We own two thin things: the `register!`/init glue in `evoxy-filter`, and a
  reference-tenancy default artifact. Both link the reused engine.
- Deployment: stock Envoy + the user's (or default) artifact + a bootstrap YAML
  mapping each logical `ClusterId` to an Envoy upstream cluster (the ADR-002
  `Target → cluster` seam).
- The same `register!` API and factory shape serve both backends (dynamic module,
  ext_proc), so switching backends does not change the user's code (ADR-001).
