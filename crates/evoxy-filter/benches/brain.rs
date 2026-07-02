//! The **shared brain** microbenchmark — the apples-to-apples cost of a request,
//! independent of backend transport (docs/12).
//!
//! Both backends run this *same* [`Filter::handle`] per request: the ext_proc
//! service's `finalize` and the dynamic module's `on_request` each build a
//! `FilterRequest`, call `handle`, and issue the effects through an
//! [`EnvoyActions`]. They differ only in **transport** — ext_proc marshals to gRPC
//! and pays an out-of-process hop (NFR-P e2e: ~3 ms added p50); the module applies
//! the effects in-process (no hop). So this bench is the compute both pay; the
//! backend choice is that compute **plus** the ext_proc IPC the module avoids.
//!
//! Instruction counts, run with `cargo xtask bench` (skips without valgrind).

use std::future::Future;
use std::hint::black_box;
use std::task::{Context, Poll, Waker};

use evoxy_abi::{FilterRequest, HttpVersion, MtlsIdentity};
use evoxy_filter::{EnvoyActions, Filter, ReferenceTenancy};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use osproxy_tenancy::TenancyRouter;

/// A no-op [`EnvoyActions`]: it records nothing beyond "did we route", so the
/// bench isolates the brain's compute, not a particular backend's materialization
/// of the effects (that difference — gRPC structs vs in-process SDK calls — is the
/// transport axis this bench deliberately excludes).
#[derive(Default)]
struct NoopActions {
    routed: bool,
}

impl EnvoyActions for NoopActions {
    fn set_upstream_cluster(&mut self, _cluster: &str) {
        self.routed = true;
    }
    fn set_upstream_host(&mut self, _host: &str) {}
    fn set_method(&mut self, _method: &str) {}
    fn set_path(&mut self, _path: &str) {}
    fn set_body(&mut self, _body: &[u8]) {}
    fn set_header(&mut self, _name: &str, _value: &str) {}
    fn remove_header(&mut self, _name: &str) {}
    fn send_local_reply(&mut self, _status: u16, _headers: &[(String, String)], _body: &[u8]) {}
}

/// A safe single-poll executor (`handle` awaits only in-memory work, never pends).
fn block_on<F: Future>(future: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(out) = future.as_mut().poll(&mut cx) {
            return out;
        }
    }
}

fn filter() -> Filter<TenancyRouter<ReferenceTenancy>> {
    Filter::new(TenancyRouter::new(ReferenceTenancy::new(
        "opensearch",
        "http://os:9200",
        "x-tenant",
    )))
}

fn request(method: &str, path: &str, body: &[u8]) -> FilterRequest {
    FilterRequest {
        method: method.to_owned(),
        path_and_query: path.to_owned(),
        authority: "os.local".to_owned(),
        version: HttpVersion::Http2,
        headers: vec![
            ("content-type".to_owned(), "application/json".to_owned()),
            ("x-request-id".to_owned(), "req-1".to_owned()),
            ("x-tenant".to_owned(), "acme".to_owned()),
        ],
        body: body.to_vec(),
        identity: MtlsIdentity::default(),
    }
}

// The whole brain for a write: adapter → resolve+transform → issue effects. This
// is what both backends run; the number is their shared per-request compute.
#[library_benchmark]
fn bench_brain_handle_write() -> bool {
    let filter = filter();
    let req = black_box(request(
        "PUT",
        "/orders/_doc/42",
        br#"{"k":1,"who":"acme"}"#,
    ));
    let mut actions = NoopActions::default();
    let _ = block_on(filter.handle(&req, &mut actions));
    black_box(actions.routed)
}

library_benchmark_group!(name = brain; benchmarks = bench_brain_handle_write);
main!(library_benchmark_groups = brain);
