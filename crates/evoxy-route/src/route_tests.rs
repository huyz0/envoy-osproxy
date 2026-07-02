//! Tests for the transform-then-forward preparation. A `TenancyRouter` wraps a
//! stub `TenancySpi`, and requests flow through the real adapter → route path,
//! mirroring how the filter will drive it (ADAPT-* → ADR-002).
// JUSTIFY: one cohesive test module tracing the ROUTE-* contract across every
// endpoint (write/read/search/count/bulk, request + response shaping); the shared
// stub tenancy + helpers keep the cases terse, so splitting would scatter them.

use evoxy_abi::{FilterRequest, HttpVersion, MtlsIdentity};
use evoxy_adapter::RequestParts;
use osproxy_core::{ClusterId, Epoch, FieldName, IndexName, PartitionId};
use osproxy_spi::{
    BodyDoc, DocIdRule, IdTemplate, InjectedField, InjectedValue, Placement, PlacementAt,
    RequestCtx, SpiError, TenancySpi,
};
use osproxy_tenancy::TenancyRouter;
use serde_json::Value;

use crate::{prepare, Forward, PreparedForward};

/// A minimal tenancy: the partition is the `x-tenant` header; placement and the
/// id rule are fixed per test.
struct StubTenancy {
    placement: Placement,
    id_rule: Option<DocIdRule>,
    /// Whether the write gate admits (false models a partition in cutover).
    admit_writes: bool,
}

impl TenancySpi for StubTenancy {
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
        self.id_rule.clone()
    }

    fn injected_fields(&self) -> Vec<InjectedField> {
        Vec::new()
    }

    async fn placement_for(&self, _partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        Ok(PlacementAt::new(self.placement.clone(), Epoch::new(1)).with_endpoint("http://os:9200"))
    }

    async fn admit_write(&self, _partition: &PartitionId, _epoch: Epoch) -> bool {
        self.admit_writes
    }
}

fn router(placement: Placement, id_rule: Option<DocIdRule>) -> TenancyRouter<StubTenancy> {
    TenancyRouter::new(StubTenancy {
        placement,
        id_rule,
        admit_writes: true,
    })
}

/// A router whose write gate is closed (the partition is in cutover).
fn blocking_router(placement: Placement) -> TenancyRouter<StubTenancy> {
    TenancyRouter::new(StubTenancy {
        placement,
        id_rule: None,
        admit_writes: false,
    })
}

fn request(method: &str, path: &str, tenant: Option<&str>, body: &[u8]) -> FilterRequest {
    let mut headers = vec![("content-type".to_owned(), "application/json".to_owned())];
    if let Some(t) = tenant {
        headers.push(("x-tenant".to_owned(), t.to_owned()));
    }
    FilterRequest {
        method: method.to_owned(),
        path_and_query: path.to_owned(),
        authority: "os.local".to_owned(),
        version: HttpVersion::Http2,
        headers,
        body: body.to_vec(),
        identity: MtlsIdentity::default(),
    }
}

// `expect` is allowed in tests; the denied `panic!` macro is not, so these
// funnel the wrong-variant case through an `Option`.
fn upstream(forward: Forward) -> PreparedForward {
    match forward {
        Forward::Upstream(prepared) => Some(prepared),
        Forward::Immediate(_) => None,
    }
    .expect("expected an Upstream forward")
}

fn immediate(forward: Forward) -> (u16, Value) {
    match forward {
        Forward::Immediate(resp) => {
            let body = serde_json::from_slice(&resp.body).expect("json error body");
            Some((resp.status, body))
        }
        Forward::Upstream(_) => None,
    }
    .expect("expected an Immediate forward")
}

fn shared(cluster: &str, index: &str, inject: Vec<InjectedField>) -> Placement {
    Placement::SharedIndex {
        cluster: ClusterId::from(cluster),
        index: IndexName::from(index),
        inject,
    }
}

#[tokio::test]
async fn dedicated_index_passes_body_and_keeps_client_id() {
    let placement = Placement::DedicatedIndex {
        cluster: ClusterId::from("eu-1"),
        index: IndexName::from("orders-p1"),
    };
    let req = request("PUT", "/orders/_doc/42", Some("acme"), br#"{"k":1}"#);
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let prepared = upstream(prepare(&router(placement, None), &parts.ctx()).await);
    assert_eq!(prepared.cluster, "eu-1");
    assert_eq!(prepared.method, "PUT");
    // logical `orders` → physical `orders-p1`, client id `42` preserved.
    assert_eq!(prepared.path, "/orders-p1/_doc/42");
    assert_eq!(prepared.body, br#"{"k":1}"#);
}

#[tokio::test]
async fn shared_index_injects_partition_and_constructs_routed_id() {
    let inject = vec![InjectedField::new(
        FieldName::from("_tenant"),
        InjectedValue::PartitionId,
    )];
    let id_rule = DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true);
    let req = request("POST", "/shared/_doc", Some("acme"), br#"{"id":1001}"#);
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let prepared = upstream(
        prepare(
            &router(shared("eu-1", "shared", inject), Some(id_rule)),
            &parts.ctx(),
        )
        .await,
    );
    assert_eq!(prepared.cluster, "eu-1");
    assert_eq!(prepared.method, "PUT");
    // The `:` in the constructed id is percent-encoded in the path.
    assert_eq!(prepared.path, "/shared/_doc/acme%3A1001?routing=acme");
    let body: Value = serde_json::from_slice(&prepared.body).unwrap();
    assert_eq!(body["_tenant"], Value::String("acme".to_owned()));
    assert_eq!(body["id"], Value::from(1001));
}

#[tokio::test]
async fn unresolved_partition_fails_closed_400() {
    let placement = Placement::DedicatedIndex {
        cluster: ClusterId::from("eu-1"),
        index: IndexName::from("orders-p1"),
    };
    let req = request("PUT", "/orders/_doc/42", None, br#"{"k":1}"#);
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let (status, body) = immediate(prepare(&router(placement, None), &parts.ctx()).await);
    assert_eq!(status, 400);
    assert_eq!(
        body["error"],
        Value::String("partition_unresolved".to_owned())
    );
}

#[tokio::test]
async fn unsupported_endpoint_is_501_without_resolving() {
    let placement = Placement::DedicatedIndex {
        cluster: ClusterId::from("eu-1"),
        index: IndexName::from("orders-p1"),
    };
    // `_delete_by_query` is not handled yet, so it fails closed without resolving.
    let req = request("POST", "/orders/_delete_by_query", Some("acme"), b"{}");
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let (status, body) = immediate(prepare(&router(placement, None), &parts.ctx()).await);
    assert_eq!(status, 501);
    assert_eq!(
        body["error"],
        Value::String("endpoint_not_supported_yet".to_owned())
    );
}

fn dedicated_index() -> Placement {
    Placement::DedicatedIndex {
        cluster: ClusterId::from("eu-1"),
        index: IndexName::from("orders-p1"),
    }
}

#[tokio::test]
async fn get_by_id_remaps_index_and_keeps_client_id() {
    let req = request("GET", "/orders/_doc/42", Some("acme"), b"");
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let prepared = upstream(prepare(&router(dedicated_index(), None), &parts.ctx()).await);
    assert_eq!(prepared.cluster, "eu-1");
    assert_eq!(prepared.method, "GET");
    // logical `orders` → physical `orders-p1`; dedicated keeps the client id.
    assert_eq!(prepared.path, "/orders-p1/_doc/42");
    assert!(prepared.body.is_empty());
}

#[tokio::test]
async fn shared_index_get_by_id_constructs_physical_id() {
    let inject = vec![InjectedField::new(
        FieldName::from("_tenant"),
        InjectedValue::PartitionId,
    )];
    let id_rule = DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true);
    let req = request("DELETE", "/shared/_doc/1001", Some("acme"), b"");
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let prepared = upstream(
        prepare(
            &router(shared("eu-1", "shared", inject), Some(id_rule)),
            &parts.ctx(),
        )
        .await,
    );
    assert_eq!(prepared.method, "DELETE");
    assert_eq!(prepared.path, "/shared/_doc/acme%3A1001?routing=acme");
}

#[tokio::test]
async fn slash_bearing_id_is_percent_encoded_in_path() {
    // A URI principal (a SPIFFE id) makes the constructed id contain `/` and `:`;
    // both are percent-encoded so the id stays a single path segment.
    let inject = vec![InjectedField::new(
        FieldName::from("_tenant"),
        InjectedValue::PartitionId,
    )];
    let id_rule = DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true);
    let req = request(
        "POST",
        "/shared/_doc",
        Some("spiffe://td/acme"),
        br#"{"id":1}"#,
    );
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let prepared = upstream(
        prepare(
            &router(shared("eu-1", "shared", inject), Some(id_rule)),
            &parts.ctx(),
        )
        .await,
    );
    assert_eq!(
        prepared.path,
        "/shared/_doc/spiffe%3A%2F%2Ftd%2Facme%3A1?routing=spiffe%3A%2F%2Ftd%2Facme"
    );
}

#[tokio::test]
async fn search_dedicated_passes_query_through() {
    let req = request(
        "POST",
        "/orders/_search",
        Some("acme"),
        br#"{"query":{"match_all":{}}}"#,
    );
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let prepared = upstream(prepare(&router(dedicated_index(), None), &parts.ctx()).await);
    assert_eq!(prepared.method, "POST");
    assert_eq!(prepared.path, "/orders-p1/_search");
    // No injected isolation fields → the query is unchanged.
    assert_eq!(prepared.body, br#"{"query":{"match_all":{}}}"#);
}

#[tokio::test]
async fn search_shared_injects_partition_filter() {
    let inject = vec![InjectedField::new(
        FieldName::from("_tenant"),
        InjectedValue::PartitionId,
    )];
    let id_rule = DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true);
    let req = request(
        "POST",
        "/shared/_search",
        Some("acme"),
        br#"{"query":{"match_all":{}}}"#,
    );
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let prepared = upstream(
        prepare(
            &router(shared("eu-1", "shared", inject), Some(id_rule)),
            &parts.ctx(),
        )
        .await,
    );
    assert_eq!(prepared.path, "/shared/_search");
    // The mandatory partition filter is now in the query (ADR-006).
    let body: Value = serde_json::from_slice(&prepared.body).unwrap();
    let filters = body["query"]["bool"]["filter"]
        .as_array()
        .expect("a filter clause");
    assert!(
        filters
            .iter()
            .any(|f| f["term"]["_tenant"] == Value::String("acme".to_owned())),
        "partition term missing: {body}"
    );
}

#[tokio::test]
async fn count_routes_to_physical_index() {
    let req = request("GET", "/orders/_count", Some("acme"), b"");
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let prepared = upstream(prepare(&router(dedicated_index(), None), &parts.ctx()).await);
    assert_eq!(prepared.path, "/orders-p1/_count");
}

fn shared_router() -> TenancyRouter<StubTenancy> {
    let inject = vec![InjectedField::new(
        FieldName::from("_tenant"),
        InjectedValue::PartitionId,
    )];
    let id_rule = DocIdRule::new(IdTemplate::new("{partition}:{body.id}")).with_routing(true);
    router(shared("eu-1", "shared", inject), Some(id_rule))
}

#[tokio::test]
async fn shape_get_response_strips_injected_field_and_unmaps() {
    let req = request("GET", "/shared/_doc/1001", Some("acme"), b"");
    let parts = RequestParts::from_filter(&req, "r").unwrap();
    let resolved = shared_router().resolve(&parts.ctx()).await.unwrap();

    let upstream = br#"{"_index":"shared","_id":"acme:1001","found":true,
                       "_source":{"_tenant":"acme","k":1}}"#;
    let shaped = crate::shape_get_response(&resolved, "shared", "1001", upstream).unwrap();

    let v: Value = serde_json::from_slice(&shaped).unwrap();
    assert_eq!(v["_index"], Value::String("shared".to_owned()));
    assert_eq!(v["_id"], Value::String("1001".to_owned())); // logical id restored
    assert!(v["_source"].get("_tenant").is_none()); // isolation field stripped
    assert_eq!(v["_source"]["k"], Value::from(1));
}

#[tokio::test]
async fn shape_search_response_shapes_each_hit() {
    let req = request("POST", "/shared/_search", Some("acme"), b"{}");
    let parts = RequestParts::from_filter(&req, "r").unwrap();
    let resolved = shared_router().resolve(&parts.ctx()).await.unwrap();

    let upstream = br#"{"took":1,"hits":{"total":{"value":1},"hits":[
        {"_index":"shared","_id":"acme:1001","_source":{"_tenant":"acme","k":1}}
    ]}}"#;
    let shaped = crate::shape_search_response(&resolved, "shared", upstream).unwrap();

    let v: Value = serde_json::from_slice(&shaped).unwrap();
    let hit = &v["hits"]["hits"][0];
    assert_eq!(hit["_index"], Value::String("shared".to_owned()));
    assert_eq!(hit["_id"], Value::String("1001".to_owned()));
    assert!(hit["_source"].get("_tenant").is_none());
    assert_eq!(hit["_source"]["k"], Value::from(1));
}

#[tokio::test]
async fn bulk_rewrites_each_item_for_shared_index() {
    let body =
        b"{\"index\":{}}\n{\"id\":1,\"who\":\"a\"}\n{\"index\":{}}\n{\"id\":2,\"who\":\"b\"}\n";
    let req = request("POST", "/orders/_bulk", Some("acme"), body);
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let prepared = upstream(prepare(&shared_router(), &parts.ctx()).await);
    assert_eq!(prepared.method, "POST");
    assert_eq!(prepared.path, "/_bulk");

    let text = String::from_utf8(prepared.body).unwrap();
    let lines: Vec<Value> = text
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 4, "two action + two source lines: {text}");

    // First item: action line has the physical index + partition-scoped id; the
    // source has the isolation field injected.
    assert_eq!(
        lines[0]["index"]["_index"],
        Value::String("shared".to_owned())
    );
    assert_eq!(lines[0]["index"]["_id"], Value::String("acme:1".to_owned()));
    assert_eq!(
        lines[0]["index"]["routing"],
        Value::String("acme".to_owned())
    );
    assert_eq!(lines[1]["_tenant"], Value::String("acme".to_owned()));
    assert_eq!(lines[1]["who"], Value::String("a".to_owned()));
    // Second item is scoped independently.
    assert_eq!(lines[2]["index"]["_id"], Value::String("acme:2".to_owned()));
    assert_eq!(lines[3]["_tenant"], Value::String("acme".to_owned()));
}

#[tokio::test]
async fn mget_rewrites_each_doc_for_shared_index() {
    let body = br#"{"docs":[{"_id":"1"},{"_id":"2","routing":"ignored"}]}"#;
    let req = request("POST", "/orders/_mget", Some("acme"), body);
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let prepared = upstream(prepare(&shared_router(), &parts.ctx()).await);
    assert_eq!(prepared.method, "POST");
    assert_eq!(prepared.path, "/_mget");

    let v: Value = serde_json::from_slice(&prepared.body).unwrap();
    let docs = v["docs"].as_array().unwrap();
    // Each fetch is pinned to the physical index with a partition-scoped id.
    assert_eq!(docs[0]["_index"], Value::String("shared".to_owned()));
    assert_eq!(docs[0]["_id"], Value::String("acme:1".to_owned()));
    assert_eq!(docs[0]["routing"], Value::String("acme".to_owned()));
    assert_eq!(docs[1]["_id"], Value::String("acme:2".to_owned()));
}

#[tokio::test]
async fn msearch_pins_index_and_injects_filter() {
    let body = "{}\n{\"query\":{\"match_all\":{}}}\n{\"index\":\"other\"}\n{\"query\":{\"term\":{\"k\":1}}}\n";
    let req = request("POST", "/orders/_msearch", Some("acme"), body.as_bytes());
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let prepared = upstream(prepare(&shared_router(), &parts.ctx()).await);
    assert_eq!(prepared.path, "/_msearch");

    let text = String::from_utf8(prepared.body).unwrap();
    let lines: Vec<Value> = text
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(lines.len(), 4, "two header + two query lines: {text}");
    // Both header lines are pinned to the physical index (even the one that named
    // `other` — a client cannot escape its placement).
    assert_eq!(lines[0]["index"], Value::String("shared".to_owned()));
    assert_eq!(lines[2]["index"], Value::String("shared".to_owned()));
    // Each query carries the mandatory partition filter (ADR-006).
    for query in [&lines[1], &lines[3]] {
        let filters = query["query"]["bool"]["filter"].as_array().expect("filter");
        assert!(
            filters
                .iter()
                .any(|f| f["term"]["_tenant"] == Value::String("acme".to_owned())),
            "partition term missing: {query}"
        );
    }
}

#[tokio::test]
async fn shape_mget_response_unmaps_each_doc() {
    let req = request("POST", "/shared/_mget", Some("acme"), b"");
    let parts = RequestParts::from_filter(&req, "r").unwrap();
    let resolved = shared_router().resolve(&parts.ctx()).await.unwrap();

    let upstream = br#"{"docs":[
        {"_index":"shared","_id":"acme:1","found":true,"_source":{"_tenant":"acme","k":1}},
        {"_index":"shared","_id":"acme:2","found":false}
    ]}"#;
    let shaped = crate::shape_mget_response(&resolved, "orders", upstream).unwrap();

    let v: Value = serde_json::from_slice(&shaped).unwrap();
    let docs = v["docs"].as_array().unwrap();
    assert_eq!(docs[0]["_index"], Value::String("orders".to_owned()));
    assert_eq!(docs[0]["_id"], Value::String("1".to_owned()));
    assert!(docs[0]["_source"].get("_tenant").is_none());
    assert_eq!(docs[1]["_id"], Value::String("2".to_owned()));
}

#[tokio::test]
async fn shape_msearch_response_shapes_each_response() {
    let req = request("POST", "/shared/_msearch", Some("acme"), b"");
    let parts = RequestParts::from_filter(&req, "r").unwrap();
    let resolved = shared_router().resolve(&parts.ctx()).await.unwrap();

    let upstream = br#"{"responses":[
        {"hits":{"hits":[{"_index":"shared","_id":"acme:1","_source":{"_tenant":"acme","k":1}}]}},
        {"hits":{"hits":[{"_index":"shared","_id":"acme:9","_source":{"_tenant":"acme","k":9}}]}}
    ]}"#;
    let shaped = crate::shape_msearch_response(&resolved, "orders", upstream).unwrap();

    let v: Value = serde_json::from_slice(&shaped).unwrap();
    let responses = v["responses"].as_array().unwrap();
    let hit0 = &responses[0]["hits"]["hits"][0];
    assert_eq!(hit0["_index"], Value::String("orders".to_owned()));
    assert_eq!(hit0["_id"], Value::String("1".to_owned()));
    assert!(hit0["_source"].get("_tenant").is_none());
    assert_eq!(
        responses[1]["hits"]["hits"][0]["_id"],
        Value::String("9".to_owned())
    );
}

#[tokio::test]
async fn shape_bulk_response_unmaps_ids_and_index() {
    let req = request("POST", "/shared/_bulk", Some("acme"), b"");
    let parts = RequestParts::from_filter(&req, "r").unwrap();
    let resolved = shared_router().resolve(&parts.ctx()).await.unwrap();

    // A real `_bulk` response: each item is keyed by the verb; the physical index
    // and partition-scoped ids must come back logical.
    let upstream = br#"{"took":3,"errors":false,"items":[
        {"index":{"_index":"shared","_id":"acme:1","status":201}},
        {"delete":{"_index":"shared","_id":"acme:2","status":200}}
    ]}"#;
    let shaped = crate::shape_bulk_response(&resolved, "orders", upstream).unwrap();

    let v: Value = serde_json::from_slice(&shaped).unwrap();
    let items = v["items"].as_array().unwrap();
    assert_eq!(
        items[0]["index"]["_index"],
        Value::String("orders".to_owned())
    );
    assert_eq!(items[0]["index"]["_id"], Value::String("1".to_owned()));
    assert_eq!(items[0]["index"]["status"], Value::from(201));
    assert_eq!(
        items[1]["delete"]["_index"],
        Value::String("orders".to_owned())
    );
    assert_eq!(items[1]["delete"]["_id"], Value::String("2".to_owned()));
}

#[tokio::test]
async fn decision_shape_is_shape_only() {
    // Shared-index write: transform=both, isolation on. The shape carries kinds
    // and flags only — no partition, index, or id value.
    let req = request("POST", "/shared/_doc", Some("acme"), br#"{"id":1}"#);
    let parts = RequestParts::from_filter(&req, "r").unwrap();
    let resolved = shared_router().resolve(&parts.ctx()).await.unwrap();

    let shape = crate::decision_shape(&resolved);
    assert_eq!(shape, "transform=both;migration=settled;isolation=on");
    // No tenant values leak.
    assert!(!shape.contains("acme"));
    assert!(!shape.contains("shared"));
}

#[tokio::test]
async fn decision_shape_dedicated_has_isolation_off() {
    let req = request("GET", "/orders/_doc/1", Some("acme"), b"");
    let parts = RequestParts::from_filter(&req, "r").unwrap();
    let resolved = router(dedicated_index(), None)
        .resolve(&parts.ctx())
        .await
        .unwrap();

    assert_eq!(
        crate::decision_shape(&resolved),
        "transform=none;migration=settled;isolation=off"
    );
}

#[tokio::test]
async fn explain_reports_route_with_shape_only_decision() {
    let req = request("POST", "/shared/_search", Some("acme"), b"{}");
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let json: Value =
        serde_json::from_str(&crate::explain(&shared_router(), &parts.ctx()).await).unwrap();
    assert_eq!(json["endpoint"], Value::String("Search".to_owned()));
    assert_eq!(json["outcome"], Value::String("route".to_owned()));
    assert_eq!(
        json["decision"],
        Value::String("transform=both;migration=settled;isolation=on".to_owned())
    );
    // No tenant value leaks into the explain.
    let raw = crate::explain(&shared_router(), &parts.ctx()).await;
    assert!(!raw.contains("acme") && !raw.contains("shared"));
}

#[tokio::test]
async fn explain_carries_the_trace_id() {
    // The W3C trace-id correlates the explain with Envoy's span.
    let mut req = request("POST", "/shared/_search", Some("acme"), b"{}");
    req.headers.push((
        "traceparent".to_owned(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
    ));
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let json: Value =
        serde_json::from_str(&crate::explain(&shared_router(), &parts.ctx()).await).unwrap();
    assert_eq!(
        json["trace"],
        Value::String("4bf92f3577b34da6a3ce929d0e0e4736".to_owned())
    );
}

#[tokio::test]
async fn explain_reports_reject_for_unresolved_partition() {
    // No tenant header → the explain honestly reports the fail-closed reject.
    let req = request("PUT", "/orders/_doc/1", None, b"{}");
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let json: Value =
        serde_json::from_str(&crate::explain(&router(dedicated_index(), None), &parts.ctx()).await)
            .unwrap();
    assert_eq!(json["outcome"], Value::String("reject".to_owned()));
    assert_eq!(json["status"], Value::from(400));
    assert_eq!(
        json["code"],
        Value::String("partition_unresolved".to_owned())
    );
}

#[tokio::test]
async fn write_during_cutover_is_rejected_409() {
    // The write gate is closed (cutover): a write fails closed with a retryable 409.
    let req = request("PUT", "/orders/_doc/42", Some("acme"), br#"{"k":1}"#);
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let (status, body) =
        immediate(prepare(&blocking_router(dedicated_index()), &parts.ctx()).await);
    assert_eq!(status, 409);
    assert_eq!(body["error"], Value::String("stale_epoch".to_owned()));
}

#[tokio::test]
async fn read_during_cutover_is_allowed() {
    // The write gate never blocks reads: a search proceeds even in cutover.
    let req = request("POST", "/orders/_search", Some("acme"), b"{}");
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let prepared = upstream(prepare(&blocking_router(dedicated_index()), &parts.ctx()).await);
    assert_eq!(prepared.path, "/orders-p1/_search");
}

#[tokio::test]
async fn bulk_during_cutover_is_rejected_409() {
    // The gate covers every write path, including `_bulk`.
    let body = b"{\"index\":{}}\n{\"id\":1}\n";
    let req = request("POST", "/orders/_bulk", Some("acme"), body);
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let (status, _) = immediate(prepare(&blocking_router(dedicated_index()), &parts.ctx()).await);
    assert_eq!(status, 409);
}

#[tokio::test]
async fn resolve_cluster_returns_target_cluster() {
    let req = request("PUT", "/orders/_doc/42", Some("acme"), b"{}");
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let cluster = crate::resolve_cluster(&router(dedicated_index(), None), &parts.ctx())
        .await
        .expect("a cluster");
    assert_eq!(cluster, "eu-1");
}

#[test]
fn authority_of_strips_scheme_and_path() {
    use crate::authority_of;
    assert_eq!(
        authority_of("http://eu-1.internal:9200"),
        Some("eu-1.internal:9200".to_owned())
    );
    assert_eq!(
        authority_of("https://os.local:9200/orders"),
        Some("os.local:9200".to_owned())
    );
    assert_eq!(authority_of("host:9200"), Some("host:9200".to_owned()));
    assert_eq!(authority_of(""), None);
    assert_eq!(authority_of("http://"), None);
}

#[tokio::test]
async fn resolve_cluster_handles_cluster_level_endpoints() {
    // A `_bulk` request carries no index in the path, but the cluster still
    // resolves from the partition, so header-phase routing must not 501 it (it
    // stays in lockstep with what `prepare` forwards at the body phase).
    let req = request("POST", "/_bulk", Some("acme"), b"");
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let cluster = crate::resolve_cluster(&router(dedicated_index(), None), &parts.ctx())
        .await
        .expect("a cluster for a cluster-level endpoint");
    assert_eq!(cluster, "eu-1");
}

#[tokio::test]
async fn resolve_cluster_fails_closed_on_unresolved_partition() {
    let req = request("PUT", "/orders/_doc/42", None, b"{}");
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let err = crate::resolve_cluster(&router(dedicated_index(), None), &parts.ctx())
        .await
        .expect_err("a fail-closed response");
    assert_eq!(err.status, 400);
}

#[tokio::test]
async fn malformed_body_fails_closed_400() {
    let placement = Placement::DedicatedIndex {
        cluster: ClusterId::from("eu-1"),
        index: IndexName::from("orders-p1"),
    };
    let req = request("PUT", "/orders/_doc/42", Some("acme"), b"not json");
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let (status, body) = immediate(prepare(&router(placement, None), &parts.ctx()).await);
    assert_eq!(status, 400);
    assert_eq!(
        body["error"],
        Value::String("body_rewrite_failed".to_owned())
    );
}
