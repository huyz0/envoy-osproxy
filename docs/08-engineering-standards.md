# 08. Engineering Standards

These standards follow the same discipline as the reused osproxy engine; this
file records what is specific to this project.

## Gates (enforced by `cargo xtask ci` + `.githooks/pre-commit`)

| Gate | What it enforces |
|------|------------------|
| `fmt` | `cargo fmt --all --check` (rustfmt.toml: 100 cols, field-init/try shorthand). |
| `clippy` | `-D warnings` across all targets; `pedantic` + `all` at warn. |
| `arch` | Downward-only crate deps (INV-1): `evoxy-abi` must not depend on `evoxy-adapter`. |
| `test` | `cargo test --workspace`, unit + doctests. |
| `doc` | `cargo doc` with `-D warnings` + doctests; every public item documented (`#![deny(missing_docs)]`). |
| `budgets` | Source files ≤ 400 lines unless they carry a `// JUSTIFY` line. |
| `bench` | iai-callgrind instruction-count benches (skipped without valgrind). |
| `coverage` | Line coverage >= 90% via `cargo llvm-cov` (`xtask` excluded — build tooling, not shipped code). A separate command and CI job, not part of `ci`, because instrumented recompiles take minutes. |

## Lints that are load-bearing here

`unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented` are **deny** in all
library and test/bench code. Beyond osproxy's reliability rationale (INV-3), a
dynamic-module panic unwinds into an Envoy worker and crashes it, so this is a
safety boundary. Tests may `unwrap`/`expect` (a panic there is the failure
signal); benches may **not**, write them with `match`/`if let`.

## Commits

Conventional commits (`type(scope): lowercase description`) with a
`Co-Authored-By:` trailer; validated by `.githooks/commit-msg`. Docs/ADRs update
in the same commit as the code they describe, drift is a bug.

## Reuse discipline

The osproxy engine crates are reused **from crates.io** (pinned `=1.0.2`),
unchanged, not vendored. If a port
needs an engine change, make it in osproxy (with its own gate) and pull it
through, never fork engine logic here. This repo owns only the Envoy seam.
