//! Deterministic instruction-count microbenchmarks for the Envoy→engine seam.
//!
//! Instruction counts (not wall-clock) so the numbers are reproducible in CI and
//! regressions are visible as a diff, mirroring the osproxy sister project
//! (docs/09). Run with `cargo xtask bench` (skips cleanly without valgrind).

use std::hint::black_box;

use evoxy_abi::{FilterRequest, HttpVersion, MtlsIdentity};
use evoxy_adapter::{classify, RequestParts};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use osproxy_spi::HttpMethod;

fn sample_request() -> FilterRequest {
    FilterRequest {
        method: "PUT".to_owned(),
        path_and_query: "/orders/_doc/42?refresh=true".to_owned(),
        authority: "os.local".to_owned(),
        version: HttpVersion::Http2,
        headers: vec![
            ("content-type".to_owned(), "application/json".to_owned()),
            ("x-request-id".to_owned(), "req-42".to_owned()),
        ],
        body: br#"{"total":10,"currency":"EUR"}"#.to_vec(),
        identity: MtlsIdentity {
            presented: true,
            subject: "CN=svc-ingest".to_owned(),
            uri_sans: vec!["spiffe://td/ingest".to_owned()],
        },
    }
}

// Just the path/method classifier — the pure part on every request.
#[library_benchmark]
fn bench_classify() -> osproxy_core::EndpointKind {
    let c = classify(black_box(HttpMethod::Put), black_box("/orders/_doc/42"));
    black_box(c.endpoint)
}

// The full seam: extract owned parts from an Envoy request, then touch the ctx
// it yields (ctx borrows parts, so parts must stay live for the call).
#[library_benchmark]
fn bench_from_filter() -> usize {
    let req = black_box(sample_request());
    match RequestParts::from_filter(&req, "req-42") {
        Ok(parts) => black_box(parts.ctx().body().len()),
        Err(_) => 0,
    }
}

library_benchmark_group!(name = adapt; benchmarks = bench_classify, bench_from_filter);
main!(library_benchmark_groups = adapt);
