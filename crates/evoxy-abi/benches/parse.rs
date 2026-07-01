//! Deterministic instruction-count microbenchmarks for the per-request header
//! parsers (M4/M7 hot path): the XFCC mTLS identity and the W3C trace-id.
//!
//! Instruction counts (not wall-clock) so the numbers are reproducible in CI and
//! a regression is a visible diff (docs/09). Run with `cargo xtask bench` (skips
//! cleanly without valgrind).

use std::hint::black_box;

use evoxy_abi::{trace_id_of, MtlsIdentity};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};

// The XFCC parse runs on every request behind Envoy-terminated mTLS: quote-aware
// split of the peer element, Subject + URI SAN extraction.
#[library_benchmark]
fn bench_from_xfcc() -> bool {
    let id = MtlsIdentity::from_xfcc(black_box(
        r#"By=spiffe://td/ingress;Hash=abc123;Subject="CN=svc,O=org";URI=spiffe://td/svc"#,
    ));
    black_box(id.presented)
}

// The trace-id parse runs on every request that carries a `traceparent`.
#[library_benchmark]
fn bench_trace_id_of() -> usize {
    let t = trace_id_of(black_box(
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    ));
    black_box(t.map_or(0, str::len))
}

library_benchmark_group!(name = parse; benchmarks = bench_from_xfcc, bench_trace_id_of);
main!(library_benchmark_groups = parse);
