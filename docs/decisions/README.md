# Architecture Decision Records (ADRs)

Each ADR is an immutable record of one decision: context, options, the decision,
and why. Superseding means adding a new ADR that references the old one, never
editing history. This is the permanent, greppable rationale trail.

Many foundational decisions are **inherited** from the reused osproxy engine
e.g.
single-target search, epoch-gated migration, filtered-or-reject isolation,
shape-only observability. ADRs here record only what is *new* to the Envoy port.

| ADR | Decision |
|-----|----------|
| [001](001-extension-mechanism.md) | Extend a stock Envoy via a Rust filter (dynamic module primary on latest Envoy, `ext_proc` co-equal) behind one `RequestCtx` adapter; never fork/recompile Envoy |
| [002](002-transform-then-forward.md) | The filter runs the engine's transform stage and returns Continue so Envoy forwards with its own pool; no in-filter dispatch (never reuse the engine `Sink`) |
| [003](003-spi-packaging.md) | SPI unchanged and compiled in statically; user owns a `cdylib`/service artifact via `register!` (+ a default reference artifact); user drops the `Sink` seam (Envoy forwards); ADR-007 preserved |
