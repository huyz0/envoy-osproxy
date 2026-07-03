//! Tests for the ext_proc message processing: drive `process_message` directly
//! (no live gRPC), asserting the `ProcessingResponse` mutations the brain yields.
// JUSTIFY: one test module for the one `process_message` entry point — sync routing,
// the reserved admin surfaces, and async write mode all exercise the same function
// through shared `headers_msg`/`body_msg`/`immediate_parts` helpers; splitting by
// phase would duplicate that scaffolding across files for no gain in cohesion.

use envoy_types::pb::envoy::config::core::v3::{HeaderMap, HeaderValue};
use evoxy_filter::{Filter, ReferenceTenancy};
use osproxy_tenancy::TenancyRouter;

use crate::directives::Directives;
use crate::extproc::processing_request::Request as Req;
use crate::extproc::processing_response::Response as Resp;
use crate::extproc::{
    body_mutation, CommonResponse, HttpBody, HttpHeaders, ProcessingRequest, ProcessingResponse,
};
use crate::metrics::Metrics;
use crate::{process_message, AsyncWriteSink, StreamState};

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use osproxy_kafka::ProduceError;

/// Drive `process_message` with no async-write sink — the default for the sync-path
/// tests. The async-write tests below build their own sink and call
/// [`process_message`] directly.
async fn pm(
    filter: &Filter<TenancyRouter<ReferenceTenancy>>,
    metrics: &Metrics,
    directives: &Directives,
    admin_token: Option<&str>,
    state: &mut StreamState,
    req: ProcessingRequest,
) -> ProcessingResponse {
    process_message(filter, metrics, directives, admin_token, None, state, req).await
}

fn filter() -> Filter<TenancyRouter<ReferenceTenancy>> {
    Filter::new(TenancyRouter::new(ReferenceTenancy::new(
        "opensearch",
        "http://os:9200",
        "x-tenant",
    )))
}

fn header(key: &str, value: &str) -> HeaderValue {
    HeaderValue {
        key: key.to_owned(),
        value: value.to_owned(),
        raw_value: Vec::new(),
    }
}

fn headers_msg(pairs: &[(&str, &str)], end_of_stream: bool) -> ProcessingRequest {
    let headers = pairs.iter().map(|(k, v)| header(k, v)).collect();
    ProcessingRequest {
        request: Some(Req::RequestHeaders(HttpHeaders {
            headers: Some(HeaderMap { headers }),
            end_of_stream,
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn body_msg(body: &[u8]) -> ProcessingRequest {
    ProcessingRequest {
        request: Some(Req::RequestBody(HttpBody {
            body: body.to_vec(),
            end_of_stream: true,
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn body_common(resp: ProcessingResponse) -> CommonResponse {
    match resp.response {
        Some(Resp::RequestBody(b)) => b.response,
        _ => None,
    }
    .expect("a RequestBody response with a CommonResponse")
}

fn set_header(common: &CommonResponse, key: &str) -> Option<String> {
    common
        .header_mutation
        .as_ref()?
        .set_headers
        .iter()
        .filter_map(|opt| opt.header.as_ref())
        .find(|hv| hv.key.eq_ignore_ascii_case(key))
        .map(|hv| String::from_utf8_lossy(&hv.raw_value).into_owned())
}

fn mutated_body(common: &CommonResponse) -> Option<Vec<u8>> {
    match common.body_mutation.as_ref()?.mutation.as_ref()? {
        body_mutation::Mutation::Body(bytes) => Some(bytes.clone()),
        _ => None,
    }
}

#[tokio::test]
async fn headers_phase_continues_without_mutation() {
    let mut state = StreamState::default();
    let msg = headers_msg(&[(":method", "PUT"), (":path", "/orders/_doc/42")], false);

    let resp = pm(
        &filter(),
        &Metrics::default(),
        &Directives::default(),
        None,
        &mut state,
        msg,
    )
    .await;

    assert!(matches!(resp.response, Some(Resp::RequestHeaders(_))));
    // headers were buffered for the body phase.
    assert!(state.headers.iter().any(|(k, _)| k == ":path"));
}

#[tokio::test]
async fn body_phase_mutates_route_and_body() {
    let filter = filter();
    let mut state = StreamState::default();
    let headers = headers_msg(
        &[
            (":method", "PUT"),
            (":path", "/orders/_doc/42"),
            ("x-tenant", "acme"),
            ("x-request-id", "req-1"),
        ],
        false,
    );
    let _ = pm(
        &filter,
        &Metrics::default(),
        &Directives::default(),
        None,
        &mut state,
        headers,
    )
    .await;

    let resp = pm(
        &filter,
        &Metrics::default(),
        &Directives::default(),
        None,
        &mut state,
        body_msg(br#"{"k":1}"#),
    )
    .await;
    let common = body_common(resp);

    // Cluster header records the routing decision; the body is rewritten.
    assert_eq!(
        set_header(&common, "x-evoxy-cluster").as_deref(),
        Some("opensearch")
    );
    assert_eq!(mutated_body(&common).as_deref(), Some(&b"{\"k\":1}"[..]));
    // The route cache is NOT cleared: with the static route, clearing it would
    // re-match on the transiently-empty `:path` (see actions::finish).
    assert!(!common.clear_route_cache);
    // The reference tenancy leaves the request line unchanged, so `:method` and
    // `:path` are NOT re-emitted (re-emitting an unchanged `:path` would empty it).
    assert_eq!(set_header(&common, ":method"), None);
    assert_eq!(set_header(&common, ":path"), None);
}

#[tokio::test]
async fn over_cap_request_body_is_refused_413() {
    let filter = filter();
    // A tiny cap so a modest body trips it; the brain never runs.
    let mut state = StreamState::new(4);
    let headers = headers_msg(
        &[
            (":method", "POST"),
            (":path", "/orders/_bulk"),
            ("x-tenant", "acme"),
        ],
        false,
    );
    let _ = pm(
        &filter,
        &Metrics::default(),
        &Directives::default(),
        None,
        &mut state,
        headers,
    )
    .await;

    let resp = pm(
        &filter,
        &Metrics::default(),
        &Directives::default(),
        None,
        &mut state,
        body_msg(br#"{"k":1}"#),
    )
    .await;
    let status = match resp.response {
        Some(Resp::ImmediateResponse(immediate)) => immediate.status.map(|s| s.code),
        _ => None,
    };
    assert_eq!(status, Some(413));
}

#[tokio::test]
async fn body_at_cap_is_allowed() {
    let filter = filter();
    let body = br#"{"k":1}"#;
    // Cap exactly at the body length: the boundary is inclusive (not refused).
    let mut state = StreamState::new(body.len());
    let headers = headers_msg(
        &[
            (":method", "PUT"),
            (":path", "/orders/_doc/42"),
            ("x-tenant", "acme"),
        ],
        false,
    );
    let _ = pm(
        &filter,
        &Metrics::default(),
        &Directives::default(),
        None,
        &mut state,
        headers,
    )
    .await;

    let resp = pm(
        &filter,
        &Metrics::default(),
        &Directives::default(),
        None,
        &mut state,
        body_msg(body),
    )
    .await;
    // Not a 413 — the body is transformed as usual.
    assert!(matches!(resp.response, Some(Resp::RequestBody(_))));
}

#[tokio::test]
async fn metrics_path_is_answered_and_counts_outcomes() {
    let filter = filter();
    let metrics = Metrics::default();

    // A routed write, then a rejected one (no tenant) — the counters move.
    let mut s1 = StreamState::default();
    let h1 = headers_msg(
        &[
            (":method", "PUT"),
            (":path", "/orders/_doc/1"),
            ("x-tenant", "acme"),
        ],
        false,
    );
    let _ = pm(&filter, &metrics, &Directives::default(), None, &mut s1, h1).await;
    let _ = pm(
        &filter,
        &metrics,
        &Directives::default(),
        None,
        &mut s1,
        body_msg(br#"{"k":1}"#),
    )
    .await;

    let mut s2 = StreamState::default();
    let h2 = headers_msg(&[(":method", "PUT"), (":path", "/orders/_doc/2")], false);
    let _ = pm(&filter, &metrics, &Directives::default(), None, &mut s2, h2).await;
    let _ = pm(
        &filter,
        &metrics,
        &Directives::default(),
        None,
        &mut s2,
        body_msg(br#"{"k":1}"#),
    )
    .await;

    // The reserved path is answered directly with a shape-only snapshot; it is not
    // itself counted.
    let mut s3 = StreamState::default();
    let probe = headers_msg(&[(":method", "GET"), (":path", "/_evoxy/metrics")], true);
    let resp = pm(
        &filter,
        &metrics,
        &Directives::default(),
        None,
        &mut s3,
        probe,
    )
    .await;
    let immediate = match resp.response {
        Some(Resp::ImmediateResponse(immediate)) => Some(immediate),
        _ => None,
    }
    .expect("an immediate metrics response");
    assert_eq!(immediate.status.map(|s| s.code), Some(200));
    assert_eq!(
        String::from_utf8(immediate.body).unwrap(),
        r#"{"requests":2,"routed":1,"rejected":1}"#
    );
}

#[tokio::test]
async fn unresolved_partition_yields_immediate_response() {
    let filter = filter();
    let mut state = StreamState::default();
    let headers = headers_msg(&[(":method", "PUT"), (":path", "/orders/_doc/42")], false);
    let _ = pm(
        &filter,
        &Metrics::default(),
        &Directives::default(),
        None,
        &mut state,
        headers,
    )
    .await;

    let resp = pm(
        &filter,
        &Metrics::default(),
        &Directives::default(),
        None,
        &mut state,
        body_msg(br#"{"k":1}"#),
    )
    .await;

    let status = match resp.response {
        Some(Resp::ImmediateResponse(immediate)) => immediate.status.map(|s| s.code),
        _ => None,
    };
    assert_eq!(status, Some(400));
}

/// Drive one reserved-path request and return its immediate `(status, body)`.
async fn admin_probe(
    directives: &Directives,
    token: Option<&str>,
    path: &str,
    auth: Option<&str>,
) -> (i32, String) {
    let mut pairs = vec![(":method", "POST"), (":path", path)];
    if let Some(a) = auth {
        pairs.push(("authorization", a));
    }
    let mut state = StreamState::default();
    let resp = pm(
        &filter(),
        &Metrics::default(),
        directives,
        token,
        &mut state,
        headers_msg(&pairs, true),
    )
    .await;
    let immediate = match resp.response {
        Some(Resp::ImmediateResponse(immediate)) => Some(immediate),
        _ => None,
    }
    .expect("an immediate admin response");
    (
        immediate.status.map(|s| s.code).unwrap_or_default(),
        String::from_utf8(immediate.body).unwrap(),
    )
}

#[tokio::test]
async fn admin_directives_are_token_gated_and_flip_live() {
    let directives = Directives::default();

    // No token configured, or a wrong/absent bearer → fail closed 403, no change.
    let (status, _) = admin_probe(
        &directives,
        None,
        "/_evoxy/admin/directives",
        Some("Bearer s3cret"),
    )
    .await;
    assert_eq!(status, 403);
    let (status, _) = admin_probe(
        &directives,
        Some("s3cret"),
        "/_evoxy/admin/directives",
        Some("Bearer wrong"),
    )
    .await;
    assert_eq!(status, 403);
    assert!(directives.emit_decision(), "unchanged by rejected calls");

    // Correct token flips the directive live and echoes the new state.
    let (status, body) = admin_probe(
        &directives,
        Some("s3cret"),
        "/_evoxy/admin/directives?emit_decision=false",
        Some("Bearer s3cret"),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body, r#"{"emit_decision":false}"#);
    assert!(!directives.emit_decision(), "flipped live");
}

// ---- Async write mode (ADR-010) ----------------------------------------------

/// A recording async-write sink: an [`AsyncWriteSink`] that captures the
/// `(path, body)` of each acknowledged produce, as a live broker that accepted the
/// record would.
#[derive(Default)]
struct RecordingSink {
    acked: std::sync::Mutex<Vec<(String, Vec<u8>)>>,
}

impl AsyncWriteSink for RecordingSink {
    fn produce_acked<'a>(
        &'a self,
        path: &'a str,
        body: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            self.acked
                .lock()
                .unwrap()
                .push((path.to_owned(), body.to_vec()));
            Ok(())
        })
    }
}

/// A sink whose produce is never acknowledged (the broker is down): the service
/// must refuse the write rather than send a `202` it cannot back.
struct FailingSink;

impl AsyncWriteSink for FailingSink {
    fn produce_acked<'a>(
        &'a self,
        _path: &'a str,
        _body: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async {
            Err(ProduceError {
                reason: "broker unavailable",
            })
        })
    }
}

/// The `(status, body)` of an immediate response, or a panic with a clear message.
fn immediate_parts(resp: ProcessingResponse) -> (i32, String) {
    let immediate = match resp.response {
        Some(Resp::ImmediateResponse(immediate)) => Some(immediate),
        _ => None,
    }
    .expect("an immediate response");
    (
        immediate.status.map(|s| s.code).unwrap_or_default(),
        String::from_utf8(immediate.body).unwrap(),
    )
}

/// Drive a write through async mode: buffer headers (with the write-mode header),
/// then send the body. Returns the body-phase response.
async fn drive_async_write(
    filter: &Filter<TenancyRouter<ReferenceTenancy>>,
    sink: Option<&dyn AsyncWriteSink>,
    method: &str,
    path: &str,
    extra: &[(&str, &str)],
    body: &[u8],
) -> ProcessingResponse {
    let metrics = Metrics::default();
    let mut state = StreamState::default();
    let mut pairs = vec![
        (":method", method),
        (":path", path),
        ("x-evoxy-write-mode", "async"),
    ];
    pairs.extend_from_slice(extra);
    let _ = process_message(
        filter,
        &metrics,
        &Directives::default(),
        None,
        sink,
        &mut state,
        headers_msg(&pairs, false),
    )
    .await;
    process_message(
        filter,
        &metrics,
        &Directives::default(),
        None,
        sink,
        &mut state,
        body_msg(body),
    )
    .await
}

/// A dedicated-index reference tenancy: it rewrites the path to a per-tenant
/// physical index, so the request the async path produces visibly differs from the
/// raw client request (isolation applied before the produce).
fn dedicated_index_filter() -> Filter<TenancyRouter<ReferenceTenancy>> {
    evoxy_filter::reference_filter(&evoxy_filter::FilterConfig::from_json(
        r#"{"isolation":"dedicated_index","cluster":"opensearch","index_template":"orders-{partition}","partition_header":"x-tenant"}"#,
    ))
}

#[tokio::test]
async fn async_write_produces_transformed_request_and_returns_202() {
    let sink = RecordingSink::default();
    let resp = drive_async_write(
        &dedicated_index_filter(),
        Some(&sink),
        "PUT",
        "/orders/_doc/42",
        &[("x-tenant", "acme")],
        br#"{"k":1}"#,
    )
    .await;

    let (status, body) = immediate_parts(resp);
    assert_eq!(status, 202, "unexpected status; body: {body}");
    assert!(body.contains("\"status\":\"accepted\""), "202 body: {body}");
    assert!(
        body.contains("\"op_id\":\""),
        "202 body carries an op_id: {body}"
    );

    // The record produced is the *physical* request the filter transformed, not the
    // raw client request — isolation already happened before the produce.
    let acked = sink.acked.lock().unwrap();
    assert_eq!(acked.len(), 1, "exactly one acked produce");
    let (produced_path, produced_body) = &acked[0];
    assert_ne!(
        produced_path, "/orders/_doc/42",
        "the produced path is the transformed physical path"
    );
    assert!(
        produced_path.contains("orders-acme"),
        "physical path targets the per-tenant index: {produced_path}"
    );
    assert!(!produced_body.is_empty(), "the request body is produced");
}

#[tokio::test]
async fn async_write_refuses_503_when_broker_does_not_ack() {
    let resp = drive_async_write(
        &filter(),
        Some(&FailingSink),
        "PUT",
        "/orders/_doc/42",
        &[("x-tenant", "acme")],
        br#"{"k":1}"#,
    )
    .await;

    let (status, body) = immediate_parts(resp);
    assert_eq!(status, 503, "refuse, do not send a false 202");
    assert_eq!(body, r#"{"error":"fanout_unavailable"}"#);
}

#[tokio::test]
async fn async_write_refuses_503_when_no_sink_configured() {
    let resp = drive_async_write(
        &filter(),
        None,
        "PUT",
        "/orders/_doc/42",
        &[("x-tenant", "acme")],
        br#"{"k":1}"#,
    )
    .await;

    let (status, body) = immediate_parts(resp);
    assert_eq!(status, 503, "no downgrade to a silent sync write");
    assert_eq!(body, r#"{"error":"async_write_unavailable"}"#);
}

#[tokio::test]
async fn async_mode_on_a_read_is_rejected_400() {
    let sink = RecordingSink::default();
    // A read reaches the brain at the headers phase (end_of_stream, no body).
    let filter = filter();
    let mut state = StreamState::default();
    let sink_dyn: Arc<dyn AsyncWriteSink> = Arc::new(sink);
    let resp = process_message(
        &filter,
        &Metrics::default(),
        &Directives::default(),
        None,
        Some(sink_dyn.as_ref()),
        &mut state,
        headers_msg(
            &[
                (":method", "GET"),
                (":path", "/orders/_doc/42"),
                ("x-tenant", "acme"),
                ("x-evoxy-write-mode", "async"),
            ],
            true,
        ),
    )
    .await;

    let (status, body) = immediate_parts(resp);
    assert_eq!(status, 400, "a read cannot be 202-queued");
    assert_eq!(body, r#"{"error":"async_write_read_unsupported"}"#);
}

#[tokio::test]
async fn sync_write_is_unaffected_by_a_configured_sink() {
    // A normal write (no write-mode header) still forwards upstream even when a sink
    // is configured — async is strictly opt-in per request.
    let filter = filter();
    let metrics = Metrics::default();
    let mut state = StreamState::default();
    let _ = process_message(
        &filter,
        &metrics,
        &Directives::default(),
        None,
        None,
        &mut state,
        headers_msg(
            &[
                (":method", "PUT"),
                (":path", "/orders/_doc/42"),
                ("x-tenant", "acme"),
            ],
            false,
        ),
    )
    .await;
    let resp = process_message(
        &filter,
        &metrics,
        &Directives::default(),
        None,
        None,
        &mut state,
        body_msg(br#"{"k":1}"#),
    )
    .await;
    // Forwarded upstream: a RequestBody response with mutations, not a 202.
    assert!(matches!(resp.response, Some(Resp::RequestBody(_))));
}
