# Benchmarks

What the two backends cost, measured end to end through a stock Envoy into a real
OpenSearch. The short version: the dynamic module adds no measurable latency over
Envoy, ext_proc adds a couple of milliseconds for its out-of-process hop, and the
per-request transform work is microseconds, swamped by OpenSearch's own write
latency. Throughput scales with concurrency rather than inflating latency.

Every number here comes from a harness in the repo you can re-run; the exact
commands are at the end. These are the numbers from one developer machine, so read
them as ratios and shapes, not absolutes for your hardware.

## Test environment

|  | This report |
|---|---|
| CPU | Intel i5-13600KF, 20 threads |
| RAM | 31 GB |
| OS | Linux 6.18 (WSL2) |
| Envoy | stock `envoyproxy/envoy:v1.37.0` |
| OpenSearch | `opensearchproject/opensearch:2.11.1`, single node |
| Network | loopback + containerized OpenSearch |
| Build | `rustc 1.94`, `--release` for the `.so`, debug for the load driver |

Because OpenSearch and Envoy run as local containers over the host gateway, the
absolute write latencies below (~1 to 20 ms) are dominated by OpenSearch and the
container network, not by the proxy. That is the point: the proxy's own cost is the
*difference* between the proxy and a direct call, and that is what the harnesses
isolate.

## Added latency: the headline

The added-latency harnesses (`perf.rs`, `perf_module.rs`) send the same write three
ways — straight to OpenSearch, through a bare Envoy with no filter, and through the
proxy — and report the differences. Single connection, 100 samples, small body.

| path | p50 | added over direct | added over Envoy |
|---|---:|---:|---:|
| direct to OpenSearch | 1.1 to 1.3 ms | baseline | — |
| bare Envoy (no filter) | ~1.8 ms | ~0.5 to 0.7 ms | baseline |
| **dynamic module** (in-process) | ~1.6 ms | ~0.5 ms | **~0 ms** |
| **ext_proc** (out-of-process gRPC) | ~3.8 ms | ~2.5 ms | **~2.0 ms** |

The dynamic module runs inside the Envoy worker, so its overhead is within the
run-to-run noise of Envoy itself (its measured p50 landed just under bare Envoy's on
this run). ext_proc pays for a localhost gRPC round trip per request, which is the
~2 ms it adds. Connection pool reuse was 1.0 in both runs, so no cost came from
reconnecting upstream.

That difference is the whole trade-off: pick the module when latency is the
priority, ext_proc when process isolation and an independent deploy are worth ~2 ms.

## Concurrency: throughput scales, latency stays flat

The concurrency harness (`scale.rs`) drives shared-index writes (the full transform)
through ext_proc at rising concurrency, 300 samples each.

| connections | p50 | p99 | throughput |
|---:|---:|---:|---:|
| 1 | 17.9 ms | 26.4 ms | 55 rps |
| 8 | 23.7 ms | 42.8 ms | 303 rps |
| 32 | 24.6 ms | 51.7 ms | 1,111 rps |

Throughput scales **20x** from 1 to 32 connections while p50 stays roughly flat (18
to 25 ms). The tail grows ~2x, which is queueing at the single-node OpenSearch under
load, not the proxy: added concurrency buys work rather than inflating the
proxy's per-request cost. The p50 here is OpenSearch's write latency (~18 ms), not
the proxy — the proxy's contribution is the ~2 ms from the table above.

## Body size and transform cost (CPU)

End to end, the transform is invisible: OpenSearch's ~18 ms write swamps it, which is
why the live sweep above holds body size fixed. To see the transform cost itself,
`evoxy-route`'s microbenchmarks count CPU instructions for the per-request work
(deterministic, so a regression is a visible diff rather than wall-clock noise).

| operation | instructions | what it is |
|---|---:|---|
| route only, no rewrite | ~11.7k | resolve placement, build the physical path |
| single write, small body | ~18.2k | + inject `_tenant`, construct the partition-scoped id |
| single write, large body | ~71.5k | the same inject over a large document |
| search | ~18.5k | wrap the query with the partition filter |
| `_bulk` (NDJSON) | ~38.9k | rewrite every action line |
| response reshape (get / search) | ~11.0k / ~16.8k | strip injected fields, map ids back |

The rewrite adds ~6.5k instructions over pure routing (18.2k vs 11.7k), and grows
with body size (18.2k small vs 71.5k large) because the field injection splices bytes
into the document. Even the large-body case is on the order of a microsecond of CPU,
three to four orders of magnitude below the network write. For a body-keyed rewrite,
size is what matters; for routing and search, it is effectively constant.

## Choosing a backend

| | dynamic module | ext_proc |
|---|---|---|
| added latency | ~0 ms over Envoy | ~2 ms over Envoy |
| isolation | shares the Envoy worker (a panic takes it down) | separate process, independent deploy and scale |
| build | a `cdylib` (needs libclang) | a small `tonic` binary |
| when | latency-sensitive, trusted logic | isolation or independent lifecycle worth 2 ms |

Both run the identical brain, so the choice is a deployment knob, not a behavior
change.

## Reproduce

The load harnesses are `#[ignore]`d (they need Docker); the microbenchmarks need
valgrind. From the repo root:

```sh
# Added latency, both backends (ext_proc needs no image; the module needs one):
cargo test -p evoxy-extproc --test perf        -- --ignored --nocapture
cargo xtask module-image
cargo test -p evoxy-extproc --test perf_module -- --ignored --nocapture

# Concurrency sweep:
cargo test -p evoxy-extproc --test scale -- --ignored --nocapture

# Transform CPU (instruction counts):
cargo bench -p evoxy-route --bench route
```

Each load harness prints a JSON profile and a pass/fail verdict against provisional
thresholds, so the same runs gate in CI's live-integration lane as well as producing
these numbers.
