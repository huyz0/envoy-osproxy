# AGENTS.md

Orientation for AI agents on **envoy-osproxy**. A router + invariants list; it
does **not** repeat the detail in [`docs/`](docs/). On conflict, the doc wins for
its topic, fix the drift.

## What this is

The osproxy capability set, multi-tenant isolation, body reshaping, `_bulk`
demux, epoch-gated migration, shape-only observability with runtime directives,
capture, delivered as an **extension of a stock Envoy**, never a fork or
recompile of Envoy. Envoy owns the wire (TLS, HTTP codecs, pooling, LB, circuit
breaking); our Rust code is the brain, plugged in behind an Envoy extension seam.

The port is tractable because the osproxy engine already split wire from brain:
its `Pipeline::handle(&RequestCtx) -> PipelineResponse` is transport-agnostic. We
**reuse those engine crates from crates.io** (`osproxy-core`/`-spi`/`-tenancy`/
`-rewrite`, pinned) and replace only the transport, no other repository is
needed. See [`docs/00-technical-analysis.md`](docs/00-technical-analysis.md).

## Crate map (downward-only deps, INV-1)

| Crate | Role |
|-------|------|
| `evoxy-abi` | Leaf: the Envoy-facing wire model (`FilterRequest`/`FilterResponse`/`MtlsIdentity`). No internal deps. |
| `evoxy-adapter` | The one seam: `FilterRequest` → `osproxy_spi::RequestCtx`. Depends on `evoxy-abi` + reused `osproxy-core`/`-spi`. |
| `evoxy-route` | Transform-then-forward (ADR-002): `RequestCtx` → the mutated request Envoy forwards, or a fail-closed reply. Reuses `osproxy-tenancy`/`-rewrite`; never dispatches. |
| `evoxy-filter` | The SDK-agnostic brain: drives adapt → route → effects over an `EnvoyActions` seam. Generic over the tenancy `Router`. |
| `evoxy-extproc` | The out-of-process backend: a `tonic` ext_proc gRPC service over `evoxy-filter`. Pure Rust, built and tested in the gate. |
| `evoxy-module-sdk` | The reusable module toolkit: the `register!` macro + SDK binding (generic over any tenancy) + `Module` driver. Links the Envoy SDK (git); what a user's cdylib depends on. Workspace-EXCLUDED (ADR-004). |
| `evoxy-module` | The in-process backend: the **reference** dynamic-module cdylib, one `register!` over `evoxy-module-sdk`. Workspace-EXCLUDED, needs libclang. |
| `evoxy-bridge` | The async fan-out bridge (ADR-005): a mirrored request → a Kafka record over `osproxy-kafka`'s `Producer` seam. Separate deployment artifact. |
| `evoxy-bench` | Dev-only NFR-P substrate (latency/throughput/scalability summaries + judges). Owned here; not shipped. |
| `xtask` | The gate (`cargo xtask ci`). Not shipped; opts out of workspace lints. |

Reused (from crates.io, pinned `=1.0.2`, not vendored): `osproxy-core`,
`osproxy-spi`, `osproxy-tenancy`, `osproxy-rewrite`, `osproxy-kafka`. **Never**
reuse osproxy's transport/server crates; Envoy replaces those.

## Invariants (don't break)

1. **Downward-only crate deps.** `evoxy-abi` depends on nothing internal; the
   adapter is the only crate aware of both Envoy and osproxy worlds.
2. **Never fork or patch Envoy.** Ship config + a loadable artifact only (docs/00).
3. **No panics in library code.** Doubly load-bearing: a dynamic-module panic
   crashes the Envoy worker. `unwrap_used`/`expect_used`/`panic` are `deny`.
4. **Faithful mapping, not policy.** The adapter maps Envoy→`RequestCtx` and back;
   authz/tenancy decisions stay in the reused engine. Fail-closed on unknowns.
5. **Telemetry is shape-only and read-only**, inherited from osproxy (INV-6 there).
6. **Time is injected, never read directly** (docs/09); `Instant::now` is banned.
7. **Keep the gate green**, `cargo xtask ci` passes before a task is done; new
   behavior needs tests.

## Commands

Install hooks once: `scripts/setup-hooks.sh`. Then:

| Step | Command |
|------|---------|
| Full gate | `cargo xtask ci` |
| Format | `cargo fmt --all` |
| Lint | `cargo xtask clippy` |
| Tests | `cargo xtask test` |
| Docs + doctests | `cargo xtask doc` |
| Microbenchmarks | `cargo xtask bench` (needs valgrind; skips otherwise) |

## Where to read next

[`docs/00-technical-analysis.md`](docs/00-technical-analysis.md) (the approach) →
[`docs/01-architecture.md`](docs/01-architecture.md) →
[`docs/decisions/`](docs/decisions/) (ADRs) →
[`docs/11-roadmap.md`](docs/11-roadmap.md) (milestones).
