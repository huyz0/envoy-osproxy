# 12 — ext_proc vs. dynamic module: the benchmark-grounded comparison

Both backends (`evoxy-extproc`, the excluded `evoxy-module`) run the **same
brain** — `evoxy-filter::Filter::handle` (adapter → `evoxy-route::prepare` →
issue effects through `EnvoyActions`). They differ in exactly one axis:
**transport**. This doc grounds the choice in the two benchmark tiers.

## The shared compute (microbenchmark)

`Filter::handle` is what the ext_proc service's `finalize` and the dynamic
module's `on_request` each call per request. It is benched directly
(`evoxy-filter/benches/brain.rs`, instruction counts under callgrind):

| bench | instructions | note |
|---|---:|---|
| `brain_handle_write` | **≈ 13,900** | adapter + resolve + transform + issue effects (dedicated write) |

Supporting per-stage numbers (`evoxy-route/benches/route.rs`, shared-index):

| stage | instructions |
|---|---:|
| `prepare_write` (inject + construct-id + id percent-encode) | ≈ 17,500 |
| `prepare_search` (query partition-filter wrap) | ≈ 17,800 |
| `prepare_bulk` (2-item NDJSON rewrite) | ≈ 38,200 |
| `decision_shape` (the `x-evoxy-decision` string) | ≈ 1,900 |
| `shape_get` (response strip + id-unmap) | ≈ 11,000 |
| `shape_search` (per-hit reshape) | ≈ 16,800 |

Order of magnitude: a request's brain compute is ~10⁴ instructions ≈ **single-digit
microseconds**. This cost is **identical for both backends** — it is the reused
osproxy engine, not the transport.

## The difference is transport, and it is measured

- **ext_proc** marshals the request to gRPC (`ProcessingRequest`) and back
  (`ProcessingResponse` with header/body mutations) and pays an **out-of-process
  hop** — two gRPC round-trips (request + response phases, both buffered). The
  end-to-end macrobenchmark (`evoxy-extproc/tests/perf.rs`) times the *same*
  GET-by-id **three ways** to attribute the overhead rather than lump it:

  | leg | p50 (dev box) | attributed to |
  |---|---:|---|
  | baseline (direct to OpenSearch) | ≈ 1.4 ms | — |
  | envoy-only (Envoy, **no** ext_proc filter) | ≈ 2.2 ms | **Envoy's own proxying: ≈ +0.8 ms** |
  | proxy (Envoy + ext_proc filter) | ≈ 4.5 ms | **our ext_proc filter: ≈ +2.3 ms over Envoy** |

  So of the ~3 ms total added latency, ~0.8 ms is Envoy simply being a proxy and
  ~2.3 ms is our filter's out-of-process hop — and the brain compute (~µs) is
  negligible in both. The hop, not the brain and not Envoy, dominates ext_proc's
  cost.
- **dynamic module** runs the brain **in-process** on the Envoy worker and applies
  the effects directly (set the header on the map, `buffer.replace` the body) —
  **no gRPC, no hop**. Its overhead is the brain (the same ~13,900 instructions)
  plus a handful of in-process SDK calls: ~microseconds, no added milliseconds. (A
  live module e2e is deferred — it needs an Envoy configured to load the `.so`; by
  construction it runs the identical brain and skips the IPC the e2e measured.)

## The verdict

Both backends are Envoy deployments, so both pay Envoy's own proxying overhead
(≈ +0.8 ms, measured above). The **differentiator** is only the filter transport:

| | brain compute | Envoy overhead (common) | filter transport (differentiator) | total added |
|---|---|---|---|---|
| **ext_proc** | ≈ 13,900 instr (~µs) | ≈ +0.8 ms | gRPC marshal + out-of-process hop: **≈ +2.3 ms** | ≈ 3 ms |
| **dynamic module** | ≈ 13,900 instr (~µs) | ≈ +0.8 ms | in-process SDK calls: **≈ +µs** | ≈ 0.8 ms |

The backend choice is **latency vs. isolation**, quantified:

- pick the **dynamic module** when the ~3 ms ext_proc hop matters (latency-sensitive
  paths) — you trade it for a shared crash blast radius (a filter panic takes the
  Envoy worker down, so the deny-`unwrap`/`panic` posture is mandatory) and a
  coupled deploy/scale lifecycle;
- pick **ext_proc** when process isolation and an independent deploy matter more
  than ~3 ms — the brain runs out-of-process, a crash is contained, and it needs no
  libclang/Envoy-header build.

Because both link the *same* `evoxy-filter` brain (ADR-001, ADR-004), this is a
deployment knob, not a rewrite — and the numbers above are why one would turn it.

## Coverage across the axes

The hot path is exercised on the axes that move its cost, each at the tier where
the signal is clean:

- **Concurrency** (e2e, `evoxy-extproc/tests/scale.rs`): the write-through-ext_proc
  path swept at c = 1, 8, 32 → an `osproxy_bench::ScalabilityCurve`. Measured
  (dev box): throughput **scales ≈ 18.6×** (57 → 1057 rps) while p50 stays roughly
  flat (17 → 23 ms) and tail amplification is **≈ 1.77×** — the filter scales by
  Envoy's pool reuse, it does not collapse. (This is an e2e axis; the ~2 ms filter
  cost is *not* isolated here because OpenSearch's ~20 ms write latency swamps it —
  rewrite cost is a microbench axis instead.)
- **Rewrite vs. no-rewrite** (micro, `evoxy-route/benches/route.rs`): a dedicated
  write (index remap only, `BodyTransform::None`) is ≈ **11,000 instr**; the shared
  write (inject `_tenant` + construct-id + id-encode) is ≈ **17,600 instr** — so the
  **rewrite itself costs ≈ 6,600 instr**, ~60 % over the no-rewrite path.
- **Body size** (micro): the same shared write with a ~4 KiB body is ≈ **71,000
  instr** vs ≈ 17,600 for a ~20 B body — ≈ **4×**, the field-inject byte-splice and
  JSON validation scaling with the body, as expected.

## Running the benchmarks

```
cargo xtask bench                                        # all microbenchmarks
cargo test -p evoxy-extproc --test perf  -- --ignored   # NFR-P A/B + Envoy-vs-filter split (Docker)
cargo test -p evoxy-extproc --test scale -- --ignored   # concurrency sweep (Docker)
```
