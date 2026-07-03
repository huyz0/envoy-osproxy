//! Brain tests: a fake `EnvoyActions` records the effects, so we assert the
//! filter's behavior without Envoy or the SDK (ADR-004). Requests flow through
//! the reference tenancy + the real adapter/route pipeline.
// JUSTIFY: one test module for the one `Filter` entry surface — handle,
// route_headers, shape_response, explain, and the passthrough/path-partition
// options all share the FakeActions recorder and request builder; splitting by
// method would duplicate that scaffolding for no gain in cohesion.

use evoxy_abi::{FilterRequest, HttpVersion, MtlsIdentity};
use osproxy_tenancy::TenancyRouter;

use crate::{EnvoyActions, Filter, FilterDecision, ReferenceTenancy};

/// Records every effect the brain issues.
#[derive(Default)]
struct FakeActions {
    cluster: Option<String>,
    host: Option<String>,
    method: Option<String>,
    path: Option<String>,
    body: Option<Vec<u8>>,
    set_headers: Vec<(String, String)>,
    removed_headers: Vec<String>,
    local_reply: Option<(u16, Vec<u8>)>,
}

impl EnvoyActions for FakeActions {
    fn set_upstream_cluster(&mut self, cluster: &str) {
        self.cluster = Some(cluster.to_owned());
    }
    fn set_upstream_host(&mut self, host: &str) {
        self.host = Some(host.to_owned());
    }
    fn set_method(&mut self, method: &str) {
        self.method = Some(method.to_owned());
    }
    fn set_path(&mut self, path: &str) {
        self.path = Some(path.to_owned());
    }
    fn set_body(&mut self, body: &[u8]) {
        self.body = Some(body.to_vec());
    }
    fn set_header(&mut self, name: &str, value: &str) {
        self.set_headers.push((name.to_owned(), value.to_owned()));
    }
    fn remove_header(&mut self, name: &str) {
        self.removed_headers.push(name.to_owned());
    }
    fn send_local_reply(&mut self, status: u16, _headers: &[(String, String)], body: &[u8]) {
        self.local_reply = Some((status, body.to_vec()));
    }
}

fn filter() -> Filter<TenancyRouter<ReferenceTenancy>> {
    Filter::new(TenancyRouter::new(ReferenceTenancy::new(
        "opensearch",
        "http://os:9200",
        "x-tenant",
    )))
}

fn request(method: &str, path: &str, tenant: Option<&str>, body: &[u8]) -> FilterRequest {
    let mut headers = vec![
        ("content-type".to_owned(), "application/json".to_owned()),
        ("x-request-id".to_owned(), "req-1".to_owned()),
    ];
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

#[tokio::test]
async fn passthrough_index_is_forwarded_unchanged() {
    // `catalog` is a passthrough index: the filter routes it upstream with no
    // partition, no transform, no cluster override. No tenant header needed.
    let f = filter().with_passthrough_indices(["catalog".to_owned()]);
    let req = request("POST", "/catalog/_search", None, br#"{"q":1}"#);
    let mut actions = FakeActions::default();

    let decision = f.handle(&req, &mut actions).await;

    assert_eq!(decision, FilterDecision::ContinueUpstream);
    assert!(actions.cluster.is_none(), "no cluster override");
    assert!(actions.path.is_none(), "path untouched");
    assert!(actions.body.is_none(), "body untouched");
    assert!(actions.local_reply.is_none());
}

#[tokio::test]
async fn path_partition_moves_the_tenant_from_the_path() {
    // Tenant in the path: `/acme/orders/_doc/42` routes as tenant `acme`, path
    // `/orders/_doc/42`. The reference tenancy (header source) resolves the injected
    // header and forwards to the placement cluster.
    let f = filter().with_path_partition_header("x-tenant");
    let req = request("PUT", "/acme/orders/_doc/42", None, br#"{"k":1}"#);
    let mut actions = FakeActions::default();

    let decision = f.handle(&req, &mut actions).await;

    assert_eq!(decision, FilterDecision::ContinueUpstream);
    assert_eq!(actions.cluster.as_deref(), Some("opensearch"));
    // The tenant segment is gone; the physical path has the logical index and id.
    assert_eq!(actions.path.as_deref(), Some("/orders/_doc/42"));
}

#[tokio::test]
async fn write_is_mutated_and_continued_upstream() {
    let req = request("PUT", "/orders/_doc/42", Some("acme"), br#"{"k":1}"#);
    let mut actions = FakeActions::default();

    let decision = filter().handle(&req, &mut actions).await;

    assert_eq!(decision, FilterDecision::ContinueUpstream);
    assert_eq!(actions.cluster.as_deref(), Some("opensearch"));
    // The placement endpoint's authority, for dynamic-forward-proxy routing.
    assert_eq!(actions.host.as_deref(), Some("os:9200"));
    assert_eq!(actions.method.as_deref(), Some("PUT"));
    // DedicatedCluster keeps the logical index and the client id.
    assert_eq!(actions.path.as_deref(), Some("/orders/_doc/42"));
    assert_eq!(actions.body.as_deref(), Some(&b"{\"k\":1}"[..]));
    assert!(actions.local_reply.is_none());
}

#[tokio::test]
async fn decision_shape_carries_trace_id() {
    // The decision header is correlated with Envoy's span by the W3C trace-id.
    let mut req = request("GET", "/orders/_doc/1", Some("acme"), b"");
    req.headers.push((
        "traceparent".to_owned(),
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
    ));
    let shape = filter()
        .decision_shape(&req)
        .await
        .expect("a decision shape");
    assert!(
        shape.ends_with(";trace=4bf92f3577b34da6a3ce929d0e0e4736"),
        "trace suffix missing: {shape}"
    );
}

#[tokio::test]
async fn write_without_mtls_is_refused_when_required() {
    // Policy on, no presented identity: a write fails closed with 403.
    let filter = filter().with_require_mtls_for_mutation(true);
    let req = request("PUT", "/orders/_doc/42", Some("acme"), br#"{"k":1}"#);
    let mut actions = FakeActions::default();

    let decision = filter.handle(&req, &mut actions).await;

    assert_eq!(decision, FilterDecision::StoppedWithLocalReply);
    let (status, body) = actions.local_reply.expect("a local reply");
    assert_eq!(status, 403);
    assert!(String::from_utf8_lossy(&body).contains("mtls_required_for_mutation"));
    // The request was never routed.
    assert!(actions.cluster.is_none());
}

#[tokio::test]
async fn read_without_mtls_is_allowed_when_required() {
    // The policy only gates writes: a read proceeds without an identity.
    let filter = filter().with_require_mtls_for_mutation(true);
    let req = request("GET", "/orders/_doc/42", Some("acme"), b"");
    let mut actions = FakeActions::default();

    let decision = filter.handle(&req, &mut actions).await;

    assert_eq!(decision, FilterDecision::ContinueUpstream);
    assert!(actions.local_reply.is_none());
}

#[tokio::test]
async fn write_with_mtls_identity_is_allowed_when_required() {
    let filter = filter().with_require_mtls_for_mutation(true);
    let mut req = request("PUT", "/orders/_doc/42", Some("acme"), br#"{"k":1}"#);
    req.identity = MtlsIdentity {
        presented: true,
        subject: "CN=svc".to_owned(),
        uri_sans: vec!["spiffe://td/svc".to_owned()],
    };
    let mut actions = FakeActions::default();

    let decision = filter.handle(&req, &mut actions).await;

    assert_eq!(decision, FilterDecision::ContinueUpstream);
    assert!(actions.local_reply.is_none());
}

#[tokio::test]
async fn unresolved_partition_sends_local_reply() {
    let req = request("PUT", "/orders/_doc/42", None, br#"{"k":1}"#);
    let mut actions = FakeActions::default();

    let decision = filter().handle(&req, &mut actions).await;

    assert_eq!(decision, FilterDecision::StoppedWithLocalReply);
    let (status, body) = actions.local_reply.expect("a local reply");
    assert_eq!(status, 400);
    assert!(actions.cluster.is_none());
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        json["error"],
        serde_json::Value::String("partition_unresolved".to_owned())
    );
}

#[tokio::test]
async fn unsupported_method_fails_closed_before_routing() {
    let req = request("PATCH", "/orders/_doc/42", Some("acme"), b"{}");
    let mut actions = FakeActions::default();

    let decision = filter().handle(&req, &mut actions).await;

    assert_eq!(decision, FilterDecision::StoppedWithLocalReply);
    let (status, body) = actions.local_reply.expect("a local reply");
    assert_eq!(status, 400);
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        json["error"],
        serde_json::Value::String("unsupported_method".to_owned())
    );
}

#[tokio::test]
async fn config_defaults_are_lenient() {
    let config = crate::reference::FilterConfig::from_json("not json");
    assert_eq!(config.cluster, "opensearch");
    assert_eq!(config.partition_header, "x-tenant");
}

#[tokio::test]
async fn route_headers_sets_only_the_cluster() {
    // The header-phase routing entry (M2c): resolve + set the cluster, nothing else.
    let req = request("PUT", "/orders/_doc/42", Some("acme"), br#"{"k":1}"#);
    let mut actions = FakeActions::default();

    let decision = filter().route_headers(&req, &mut actions).await;

    assert_eq!(decision, FilterDecision::ContinueUpstream);
    assert_eq!(actions.cluster.as_deref(), Some("opensearch"));
    // Header phase only: no path/body/method mutation, no local reply.
    assert!(actions.path.is_none());
    assert!(actions.body.is_none());
    assert!(actions.local_reply.is_none());
}

#[tokio::test]
async fn route_headers_fails_closed_when_unresolved() {
    let req = request("PUT", "/orders/_doc/42", None, br#"{"k":1}"#);
    let mut actions = FakeActions::default();

    let decision = filter().route_headers(&req, &mut actions).await;

    assert_eq!(decision, FilterDecision::StoppedWithLocalReply);
    assert!(actions.cluster.is_none());
    let (status, _) = actions.local_reply.expect("a local reply");
    assert_eq!(status, 400);
}

// ---- Deep-review coverage: response shaping, header-phase edges, helpers ------

/// A shared-index filter (isolation transform on), for the response-shaping tests.
fn shared_filter() -> Filter<TenancyRouter<ReferenceTenancy>> {
    crate::reference_filter(&crate::FilterConfig::from_json(
        r#"{"cluster":"opensearch","shared_index":"orders_shared","inject_field":"_tenant","partition_header":"x-tenant"}"#,
    ))
}

/// The response phase reshapes a shared-index read into the client's logical view
/// (id unmapped, isolation field stripped); a non-shapeable request yields `None`.
#[tokio::test]
async fn shape_response_restores_the_logical_view() {
    let filter = shared_filter();
    let req = request("GET", "/orders/_doc/42", Some("acme"), b"");
    let upstream = br#"{"_index":"orders_shared","_id":"acme:42","found":true,"_source":{"_tenant":"acme","k":1}}"#;

    let shaped = filter
        .shape_response(&req, upstream)
        .await
        .expect("a shaped read");
    let text = String::from_utf8(shaped).unwrap();
    assert!(text.contains(r#""_id":"42""#), "{text}");
    assert!(
        !text.contains("_tenant"),
        "isolation field stripped: {text}"
    );

    // A delete is not a shapeable read: the upstream body passes unchanged.
    let del = request("DELETE", "/orders/_doc/42", Some("acme"), b"");
    assert!(filter
        .shape_response(&del, br#"{"result":"deleted"}"#)
        .await
        .is_none());
}

/// `route_headers` fails closed on an unclassifiable request, before resolution.
#[tokio::test]
async fn route_headers_rejects_unsupported_method() {
    let mut actions = FakeActions::default();
    let req = request("PATCH", "/orders/_doc/1", Some("acme"), b"{}");
    let decision = filter().route_headers(&req, &mut actions).await;
    assert_eq!(decision, FilterDecision::StoppedWithLocalReply);
    let (status, body) = actions.local_reply.expect("a fail-closed reply");
    assert_eq!(status, 400);
    assert!(String::from_utf8(body)
        .unwrap()
        .contains("unsupported_method"));
}

/// A passthrough index skips cluster resolution at the header phase too, and the
/// path-partition rewrite (when on) is still applied to the forwarded path.
#[tokio::test]
async fn route_headers_passthrough_applies_the_path_rewrite() {
    let filter = filter()
        .with_passthrough_indices(["catalog".to_owned()])
        .with_path_partition_header("x-tenant");
    let mut actions = FakeActions::default();
    // Path source: /acme/catalog/... routes as tenant acme, path /catalog/...
    let req = request("GET", "/acme/catalog/_doc/1?pretty=true", None, b"");
    let decision = filter.route_headers(&req, &mut actions).await;
    assert_eq!(decision, FilterDecision::ContinueUpstream);
    assert_eq!(actions.cluster, None, "passthrough resolves no cluster");
    // The stripped path (query preserved) is what Envoy forwards.
    assert_eq!(actions.path.as_deref(), Some("/catalog/_doc/1?pretty=true"));
}

/// `handle` applies the same passthrough path rewrite at the body phase.
#[tokio::test]
async fn handle_passthrough_applies_the_path_rewrite() {
    let filter = filter()
        .with_passthrough_indices(["catalog".to_owned()])
        .with_path_partition_header("x-tenant");
    let mut actions = FakeActions::default();
    let req = request("GET", "/acme/catalog/_doc/1", None, b"");
    let decision = filter.handle(&req, &mut actions).await;
    assert_eq!(decision, FilterDecision::ContinueUpstream);
    assert_eq!(actions.path.as_deref(), Some("/catalog/_doc/1"));
    assert_eq!(actions.body, None, "no transform on passthrough");
}

/// `reference_filter` wires the config's passthrough and path-partition options
/// onto the filter (the config-only path exercises the same builders).
#[tokio::test]
async fn reference_filter_wires_passthrough_and_path_source() {
    let filter = crate::reference_filter(&crate::FilterConfig::from_json(
        r#"{"cluster":"os","partition_source":"path","partition_header":"x-t","passthrough_indices":["catalog"]}"#,
    ));
    let mut actions = FakeActions::default();
    // /globex/catalog/... → tenant globex via path, catalog is passthrough.
    let req = request("GET", "/globex/catalog/_doc/1", None, b"");
    let decision = filter.handle(&req, &mut actions).await;
    assert_eq!(decision, FilterDecision::ContinueUpstream);
    assert_eq!(actions.path.as_deref(), Some("/catalog/_doc/1"));
}

/// `explain` reports an unclassifiable request as a shape-only reject.
#[tokio::test]
async fn explain_rejects_unsupported_method() {
    let line = filter()
        .explain(&request("PATCH", "/orders/_doc/1", Some("acme"), b"{}"))
        .await;
    assert!(line.contains("unsupported_method"), "{line}");
    assert!(line.contains("400"), "{line}");
}

/// `is_write` classifies writes and reads, and an unclassifiable method is not a
/// write (async mode then refuses it as a read rather than transforming blind).
#[tokio::test]
async fn is_write_classifies_and_fails_closed() {
    let f = filter();
    assert!(f.is_write(&request("PUT", "/orders/_doc/1", Some("acme"), b"{}")));
    assert!(!f.is_write(&request("GET", "/orders/_doc/1", Some("acme"), b"")));
    assert!(!f.is_write(&request("PATCH", "/orders/_doc/1", Some("acme"), b"{}")));
}

/// Header ops (the migration seam) map onto the actions: Add/Replace set, Remove
/// removes, and an unknown future op is ignored rather than mis-applied.
#[test]
fn header_ops_map_onto_actions() {
    use osproxy_spi::HeaderOp;
    let mut actions = FakeActions::default();
    let ops = vec![
        HeaderOp::Add {
            name: "x-a".to_owned(),
            value: "1".to_owned(),
        },
        HeaderOp::Replace {
            name: "x-b".to_owned(),
            value: "2".to_owned(),
        },
        HeaderOp::Remove {
            name: "x-c".to_owned(),
        },
    ];
    crate::apply_header_ops(&ops, &mut actions);
    assert_eq!(
        actions.set_headers,
        vec![
            ("x-a".to_owned(), "1".to_owned()),
            ("x-b".to_owned(), "2".to_owned())
        ]
    );
    assert_eq!(actions.removed_headers, vec!["x-c".to_owned()]);
}

/// The leading-segment splitter keeps the query, refuses an empty tenant, and
/// refuses a path with no remainder to route on.
#[test]
fn split_leading_segment_edges() {
    assert_eq!(
        crate::split_leading_segment("/acme/orders/_search?q=1"),
        Some(("acme".to_owned(), "/orders/_search?q=1".to_owned()))
    );
    // An empty tenant segment (double slash) is not a tenant.
    assert_eq!(crate::split_leading_segment("//orders/_search"), None);
    // A bare segment has no remainder to route on.
    assert_eq!(crate::split_leading_segment("/orders"), None);
}
