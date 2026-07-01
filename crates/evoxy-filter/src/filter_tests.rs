//! Brain tests: a fake `EnvoyActions` records the effects, so we assert the
//! filter's behavior without Envoy or the SDK (ADR-004). Requests flow through
//! the reference tenancy + the real adapter/route pipeline.

use evoxy_abi::{FilterRequest, HttpVersion, MtlsIdentity};
use osproxy_tenancy::TenancyRouter;

use crate::{EnvoyActions, Filter, FilterDecision, ReferenceTenancy};

/// Records every effect the brain issues.
#[derive(Default)]
struct FakeActions {
    cluster: Option<String>,
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
async fn write_is_mutated_and_continued_upstream() {
    let req = request("PUT", "/orders/_doc/42", Some("acme"), br#"{"k":1}"#);
    let mut actions = FakeActions::default();

    let decision = filter().handle(&req, &mut actions).await;

    assert_eq!(decision, FilterDecision::ContinueUpstream);
    assert_eq!(actions.cluster.as_deref(), Some("opensearch"));
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
