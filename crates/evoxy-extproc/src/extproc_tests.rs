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
        .map(|hv| hv.value.clone())
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

    let resp = process_message(&filter(), &mut state, msg).await;

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
    let _ = process_message(&filter, &mut state, headers).await;

    let resp = process_message(&filter, &mut state, body_msg(br#"{"k":1}"#)).await;
    let common = body_common(resp);

    // Cluster header selects the upstream; the body is rewritten; route cache is
    // cleared so the header re-routes.
    assert_eq!(
        set_header(&common, "x-evoxy-cluster").as_deref(),
        Some("opensearch")
    );
    assert_eq!(mutated_body(&common).as_deref(), Some(&b"{\"k\":1}"[..]));
    assert!(common.clear_route_cache);
    // The reference tenancy leaves the request line unchanged, so `:method` and
    // `:path` are NOT re-emitted (re-emitting an unchanged `:path` would empty it).
    assert_eq!(set_header(&common, ":method"), None);
    assert_eq!(set_header(&common, ":path"), None);
}

#[tokio::test]
async fn unresolved_partition_yields_immediate_response() {
    let filter = filter();
    let mut state = StreamState::default();
    let headers = headers_msg(&[(":method", "PUT"), (":path", "/orders/_doc/42")], false);
    let _ = process_message(&filter, &mut state, headers).await;

    let resp = process_message(&filter, &mut state, body_msg(br#"{"k":1}"#)).await;

    let status = match resp.response {
        Some(Resp::ImmediateResponse(immediate)) => immediate.status.map(|s| s.code),
        _ => None,
    };
    assert_eq!(status, Some(400));
}
