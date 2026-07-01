//! Tests for the ext_proc message processing: drive `process_message` directly
//! (no live gRPC), asserting the `ProcessingResponse` mutations the brain yields.

use envoy_types::pb::envoy::config::core::v3::{HeaderMap, HeaderValue};
use evoxy_filter::{Filter, ReferenceTenancy};
use osproxy_tenancy::TenancyRouter;

use crate::extproc::processing_request::Request as Req;
use crate::extproc::processing_response::Response as Resp;
use crate::extproc::{
    body_mutation, CommonResponse, HttpBody, HttpHeaders, ProcessingRequest, ProcessingResponse,
};
use crate::metrics::Metrics;
use crate::{process_message, StreamState};

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

    let resp = process_message(&filter(), &Metrics::default(), &mut state, msg).await;

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
    let _ = process_message(&filter, &Metrics::default(), &mut state, headers).await;

    let resp = process_message(
        &filter,
        &Metrics::default(),
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
    let _ = process_message(&filter, &Metrics::default(), &mut state, headers).await;

    let resp = process_message(
        &filter,
        &Metrics::default(),
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
    let _ = process_message(&filter, &Metrics::default(), &mut state, headers).await;

    let resp = process_message(&filter, &Metrics::default(), &mut state, body_msg(body)).await;
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
    let _ = process_message(&filter, &metrics, &mut s1, h1).await;
    let _ = process_message(&filter, &metrics, &mut s1, body_msg(br#"{"k":1}"#)).await;

    let mut s2 = StreamState::default();
    let h2 = headers_msg(&[(":method", "PUT"), (":path", "/orders/_doc/2")], false);
    let _ = process_message(&filter, &metrics, &mut s2, h2).await;
    let _ = process_message(&filter, &metrics, &mut s2, body_msg(br#"{"k":1}"#)).await;

    // The reserved path is answered directly with a shape-only snapshot; it is not
    // itself counted.
    let mut s3 = StreamState::default();
    let probe = headers_msg(&[(":method", "GET"), (":path", "/_evoxy/metrics")], true);
    let resp = process_message(&filter, &metrics, &mut s3, probe).await;
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
    let _ = process_message(&filter, &Metrics::default(), &mut state, headers).await;

    let resp = process_message(
        &filter,
        &Metrics::default(),
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
