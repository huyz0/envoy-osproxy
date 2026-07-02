# ext_proc vs. dynamic module

Both backends run the same brain, the reused osproxy engine, and produce identical
routing and transforms. They differ in one thing: transport. This page gives the
measured numbers so you can pick with your eyes open.

Pick the dynamic module when latency is what matters. Pick ext_proc when process
isolation and an independent deploy are worth a couple of milliseconds.

## The difference is transport, and it is measured

Each backend was timed on the same GET-by-id three ways against one real OpenSearch,
so the overhead is attributed rather than lumped: a direct baseline, a bare Envoy
with no filter to isolate Envoy's own proxying cost, and Envoy plus the filter.

ext_proc marshals the request to gRPC and back and pays an out-of-process hop:

| leg | p50 (dev box) |
|---|---:|
| baseline (direct to OpenSearch) | about 1.4 ms |
| Envoy only, no filter | about 2.2 ms |
| Envoy plus ext_proc filter | about 4.5 ms |

The filter adds about 2.3 ms over Envoy. The hop dominates, not the compute.

The dynamic module runs the brain in-process and applies effects directly, with no
gRPC and no hop:

| leg | p50 (dev box) |
|---|---:|
| baseline (direct to OpenSearch) | about 1.3 ms |
| Envoy only, no filter | about 1.8 ms |
| Envoy plus in-process module | about 1.6 ms |

The module leg lands below the bare-Envoy leg. Its own cost is within Envoy's
run-to-run jitter, so it adds no measurable milliseconds over Envoy.

## The verdict

Both are Envoy deployments, so both pay Envoy's own overhead. The only
differentiator is the filter transport:

| backend | brain compute | filter transport | added latency |
|---|---|---|---|
| ext_proc | microseconds | gRPC plus out-of-process hop | about +2.3 ms |
| dynamic module | microseconds | in-process calls | about +0 ms, within the noise |

The dynamic module trades that latency win for a shared crash domain. A filter
panic takes the Envoy worker down, so the code's no-panic rule is not optional
there. It also couples the deploy: the tenancy is compiled into the `.so`, and
changing it means rebuilding and rolling Envoy.

ext_proc keeps the brain out-of-process. A crash is contained, the service deploys
on its own schedule, and it needs no special build toolchain. You pay the hop.

Because both run the same brain, this is a deployment knob, not a rewrite.

## Reproducing the numbers

The benchmarks run against real containers:

```sh
cargo test -p evoxy-extproc --test perf -- --ignored           # ext_proc, three legs
cargo xtask module-image
cargo test -p evoxy-extproc --test perf_module -- --ignored    # dynamic module, three legs
```

For the full picture (added latency, the concurrency sweep, and the per-request
transform cost by body size), see [Benchmarks](06-benchmarks.md).
