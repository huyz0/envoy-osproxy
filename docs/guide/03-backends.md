# ext_proc vs. dynamic module

Both backends run the **same brain** — the reused osproxy engine — and produce
identical routing and transforms. They differ in exactly one axis: **transport**.
This page grounds the choice in measured numbers so you can pick with your eyes
open.

## The difference is transport, and it is measured

Each backend was timed on the *same* GET-by-id **three ways** against one real
OpenSearch, so the overhead is attributed rather than lumped: a direct baseline, a
bare Envoy with no filter (Envoy's own proxying cost), and Envoy plus our filter.

**ext_proc** marshals the request to gRPC and back and pays an out-of-process hop:

| leg | p50 (dev box) |
|---|---:|
| baseline (direct to OpenSearch) | ≈ 1.4 ms |
| Envoy only (no filter) | ≈ 2.2 ms |
| Envoy + ext_proc filter | ≈ 4.5 ms |

→ the filter adds **≈ +2.3 ms** over Envoy — the out-of-process hop, not the
compute, dominates.

**dynamic module** runs the brain in-process and applies effects directly — no
gRPC, no hop:

| leg | p50 (dev box) |
|---|---:|
| baseline (direct to OpenSearch) | ≈ 1.3 ms |
| Envoy only (no filter) | ≈ 1.8 ms |
| Envoy + in-process module | ≈ 1.6 ms |

→ the module leg lands *below* the bare-Envoy leg — its own cost is within Envoy's
run-to-run jitter, so it adds **no measurable milliseconds** over Envoy.

## The verdict

Both are Envoy deployments, so both pay Envoy's own overhead. The only
differentiator is the filter transport:

| | brain compute | filter transport | added latency |
|---|---|---|---|
| **ext_proc** | ~microseconds | gRPC + out-of-process hop | **≈ +2.3 ms** |
| **dynamic module** | ~microseconds | in-process calls | **≈ +0 ms (in the noise)** |

The choice is **latency vs. isolation**:

- pick the **dynamic module** when latency matters most — you trade it for a shared
  crash domain (a filter panic takes the Envoy worker down) and a coupled deploy;
- pick **ext_proc** when process isolation and an independent deploy matter more
  than a couple of milliseconds — the brain runs out-of-process, a crash is
  contained, and it needs no special build toolchain.

Because both run the same brain, this is a deployment knob, not a rewrite.

## How to reproduce

The benchmarks are in the repository and run against real containers:

```sh
cargo test -p evoxy-extproc --test perf        -- --ignored   # ext_proc, 3-leg
cargo xtask module-image
cargo test -p evoxy-extproc --test perf_module -- --ignored   # dynamic module, 3-leg
```
