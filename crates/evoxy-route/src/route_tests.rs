//! Tests for the transform-then-forward preparation. A `TenancyRouter` wraps a
//! stub `TenancySpi`, and requests flow through the real adapter → route path,
//! mirroring how the filter will drive it (ADAPT-* → ADR-002).

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
}

fn router(placement: Placement, id_rule: Option<DocIdRule>) -> TenancyRouter<StubTenancy> {
    TenancyRouter::new(StubTenancy { placement, id_rule })
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
    assert_eq!(prepared.path, "/shared/_doc/acme:1001?routing=acme");
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
    // `_mget` is not handled yet, so it fails closed without resolving.
    let req = request("POST", "/orders/_mget", Some("acme"), b"{}");
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
    assert_eq!(prepared.path, "/shared/_doc/acme:1001?routing=acme");
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
async fn resolve_cluster_returns_target_cluster() {
    let req = request("PUT", "/orders/_doc/42", Some("acme"), b"{}");
    let parts = RequestParts::from_filter(&req, "r").unwrap();

    let cluster = crate::resolve_cluster(&router(dedicated_index(), None), &parts.ctx())
        .await
        .expect("a cluster");
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
