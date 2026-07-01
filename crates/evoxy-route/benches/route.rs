//! Deterministic instruction-count microbenchmarks for the transform-then-forward
//! hot path (ADR-002): `prepare` over a stub tenancy, per endpoint family.
//!
//! This is the per-request work the ext_proc body phase drives — resolve the
//! placement, apply the body transform (inject fields, construct the id), wrap the
//! query filter, rewrite `_bulk` NDJSON, and build the physical path (with id
//! percent-encoding). Instruction counts (not wall-clock) so a regression is a
//! visible diff (docs/09). Run with `cargo xtask bench` (skips without valgrind).

use std::future::Future;
use std::hint::black_box;
use std::task::{Context, Poll, Waker};

use evoxy_abi::{FilterRequest, HttpVersion, MtlsIdentity};
use evoxy_adapter::RequestParts;
use evoxy_route::{prepare, Forward};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use osproxy_core::{ClusterId, Epoch, FieldName, IndexName, PartitionId};
use osproxy_spi::{
    BodyDoc, DocIdRule, IdTemplate, InjectedField, InjectedValue, Placement, PlacementAt,
    RequestCtx, SpiError, TenancySpi,
};
use osproxy_tenancy::TenancyRouter;

/// A shared-index tenancy (inject `_tenant` + partition-scoped id) — the heavier,
/// more representative transform path.
struct Stub;

impl TenancySpi for Stub {
    fn resolve_partition(
        &self,
        ctx: &RequestCtx<'_>,
        _body: BodyDoc<'_>,
    ) -> Result<PartitionId, SpiError> {
        ctx.headers()
            .get("x-tenant")
            .map(PartitionId::from)
            .ok_or(SpiError::PartitionUnresolved { tried: Vec::new() })
    }

    fn doc_id_rule(&self) -> Option<DocIdRule> {
        Some(DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true))
    }

    fn injected_fields(&self) -> Vec<InjectedField> {
        vec![InjectedField::new(
            FieldName::from("_tenant"),
            InjectedValue::PartitionId,
        )]
    }

    async fn placement_for(&self, _partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        Ok(PlacementAt::new(
            Placement::SharedIndex {
                cluster: ClusterId::from("eu-1"),
                index: IndexName::from("shared"),
                inject: self.injected_fields(),
            },
            Epoch::new(1),
        )
        .with_endpoint("http://os:9200"))
    }
}

fn router() -> TenancyRouter<Stub> {
    TenancyRouter::new(Stub)
}

fn request(method: &str, path: &str, body: &[u8]) -> FilterRequest {
    FilterRequest {
        method: method.to_owned(),
        path_and_query: path.to_owned(),
        authority: "os.local".to_owned(),
        version: HttpVersion::Http2,
        headers: vec![
            ("content-type".to_owned(), "application/json".to_owned()),
            ("x-tenant".to_owned(), "acme".to_owned()),
        ],
        body: body.to_vec(),
        identity: MtlsIdentity::default(),
    }
}

/// A safe single-poll executor: `prepare` awaits only in-memory work (no I/O), so
/// it is ready on the first poll — no runtime needed, no `unsafe`.
fn block_on<F: Future>(future: F) -> F::Output {
    let mut cx = Context::from_waker(Waker::noop());
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(out) = future.as_mut().poll(&mut cx) {
            return out;
        }
    }
}

/// Drive `prepare` for one request and report whether it routed (keeps the whole
/// pipeline live under `black_box`).
fn drive(req: &FilterRequest) -> bool {
    let router = router();
    match RequestParts::from_filter(req, "r") {
        Ok(parts) => matches!(
            block_on(prepare(&router, &parts.ctx())),
            Forward::Upstream(_)
        ),
        Err(_) => false,
    }
}

// Single-doc write: inject `_tenant`, construct `acme:1001`, percent-encode the id.
#[library_benchmark]
fn bench_prepare_write() -> bool {
    let req = black_box(request(
        "PUT",
        "/shared/_doc",
        br#"{"id":1001,"who":"acme"}"#,
    ));
    black_box(drive(&req))
}

// Search: wrap the query with the mandatory partition filter.
#[library_benchmark]
fn bench_prepare_search() -> bool {
    let req = black_box(request(
        "POST",
        "/shared/_search",
        br#"{"query":{"match_all":{}}}"#,
    ));
    black_box(drive(&req))
}

// `_bulk`: rewrite each NDJSON item (inject + construct id + physical index).
#[library_benchmark]
fn bench_prepare_bulk() -> bool {
    let body =
        b"{\"index\":{}}\n{\"id\":1,\"who\":\"a\"}\n{\"index\":{}}\n{\"id\":2,\"who\":\"b\"}\n";
    let req = black_box(request("POST", "/shared/_bulk", body));
    black_box(drive(&req))
}

library_benchmark_group!(
    name = route;
    benchmarks = bench_prepare_write, bench_prepare_search, bench_prepare_bulk
);
main!(library_benchmark_groups = route);
