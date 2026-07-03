//! The ext_proc backend (ADR-001): an Envoy External Processing gRPC service
//! over the same [`evoxy_filter`] brain.
//!
//! Envoy streams each request's headers then body to this service; we assemble a
//! [`FilterRequest`](evoxy_abi::FilterRequest), run the brain, and stream back a
//! `ProcessingResponse` that mutates the request (`:method`/`:path`, the
//! [`CLUSTER_HEADER`] routing header, and the body) or replies immediately
//! (fail-closed). It **never dispatches** — Envoy forwards the mutated request to
//! the cluster the header selects (ADR-002).
//!
//! Pure Rust over `tonic` — no `libclang`, so unlike the dynamic module it builds
//! and is tested in the gate. Both backends share `evoxy-filter`, so the choice
//! is a deployment knob (ADR-001).
#![deny(missing_docs)]
// JUSTIFY: the ext_proc service's single request/response narrative —
// `process_message`'s phase dispatch plus the response builders it hands back
// (route mutation, response reshape, and the reserved admin surfaces it answers:
// /metrics, /explain, /admin/directives). The stateful bits already live in their
// own modules (actions/convert/metrics/directives); what remains is the one
// message-handling flow, which reads worse split mid-`match`.

mod actions;
mod asyncwrite;
mod convert;
mod directives;
mod metrics;
mod service;

/// The generated Envoy ext_proc v3 types.
pub(crate) use envoy_types::pb::envoy::service::ext_proc::v3 as extproc;

pub use actions::CLUSTER_HEADER;
pub use asyncwrite::{AsyncWriteSink, WRITE_MODE_HEADER};
pub use service::{ExtProcService, ExternalProcessorServer};

use actions::ExtProcActions;
use directives::{constant_time_eq, Directives, ADMIN_PATH};
use envoy_types::pb::envoy::config::core::v3::{HeaderValue, HeaderValueOption};
use envoy_types::pb::envoy::r#type::v3::HttpStatus;
use evoxy_filter::Filter;
use extproc::processing_request::Request as Req;
use extproc::processing_response::Response as Resp;
use extproc::{
    BodyResponse, HeadersResponse, ImmediateResponse, ProcessingRequest, ProcessingResponse,
};
use metrics::{Metrics, METRICS_PATH};
use osproxy_tenancy::Router;

/// Default cap on a request body the service will buffer and transform, bounding
/// the per-request working set (the transform-then-forward model must hold the
/// whole body to rewrite it, so an unbounded body is an unbounded allocation). A
/// body over the cap is refused with `413` before the brain runs. Configurable
/// via [`ExtProcService::with_max_request_body_bytes`].
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 32 * 1024 * 1024;

/// Per-stream state: the request headers, buffered from the headers phase so the
/// body phase can build the full request, and the request-body cap.
struct StreamState {
    headers: Vec<(String, String)>,
    max_request_body_bytes: usize,
}

impl StreamState {
    /// A fresh per-stream state with the given request-body cap.
    fn new(max_request_body_bytes: usize) -> Self {
        Self {
            headers: Vec::new(),
            max_request_body_bytes,
        }
    }
}

impl Default for StreamState {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_REQUEST_BODY_BYTES)
    }
}

/// Process one ext_proc message, producing the response Envoy expects for that
/// phase. Headers are buffered; the request is resolved+mutated at the body phase
/// (a headerless request — a read — is resolved at the headers phase).
async fn process_message<R: Router>(
    filter: &Filter<R>,
    metrics: &Metrics,
    directives: &Directives,
    admin_token: Option<&str>,
    async_sink: Option<&dyn AsyncWriteSink>,
    state: &mut StreamState,
    request: ProcessingRequest,
) -> ProcessingResponse {
    match request.request {
        Some(Req::RequestHeaders(headers)) => {
            state.headers = convert::extract_headers(&headers);
            // The reserved admin paths (M7) are answered by the filter itself,
            // short-circuited before any routing.
            if let Some(resp) =
                reserved_response(filter, metrics, directives, admin_token, &state.headers).await
            {
                return resp;
            }
            if headers.end_of_stream {
                // A bodyless write (e.g. delete-by-id): async mode queues it here.
                dispatch_write(
                    filter,
                    metrics,
                    async_sink,
                    state.headers.clone(),
                    Vec::new(),
                    Phase::Headers,
                )
                .await
            } else {
                // Continue; the mutation happens once we have the body. Envoy
                // requires a `CommonResponse` (an empty response is rejected).
                wrap(Resp::RequestHeaders(HeadersResponse {
                    response: Some(extproc::CommonResponse::default()),
                }))
            }
        }
        Some(Req::RequestBody(body)) => {
            // Bound the working set: the transform holds the whole body, so refuse
            // an over-cap body up front (fail-closed) rather than allocate for it.
            if body.body.len() > state.max_request_body_bytes {
                metrics.record_rejected();
                return payload_too_large();
            }
            dispatch_write(
                filter,
                metrics,
                async_sink,
                state.headers.clone(),
                body.body,
                Phase::Body,
            )
            .await
        }
        // Response path (M2b): reshape a read's response into the client's logical
        // view. Headers just continue; the body is shaped (strip injected fields,
        // map physical ids back to logical) using the buffered request headers.
        Some(Req::ResponseHeaders(_)) => {
            // Surface the shape-only routing decision (M7) as a response header,
            // the "why did this route here" the extension knows and Envoy cannot —
            // unless an operator has silenced it via the directive plane.
            if directives.emit_decision() {
                let req = convert::filter_request(state.headers.clone(), Vec::new());
                response_headers(filter.decision_shape(&req).await)
            } else {
                response_headers(None)
            }
        }
        Some(Req::ResponseBody(body)) => {
            let req = convert::filter_request(state.headers.clone(), Vec::new());
            let shaped = filter.shape_response(&req, &body.body).await;
            response_body(shaped)
        }
        // Trailers etc.: continue unmodified.
        _ => wrap(Resp::RequestBody(BodyResponse {
            response: Some(extproc::CommonResponse::default()),
        })),
    }
}

/// The reserved admin paths, answered by the filter itself (M7): `/_evoxy/metrics`
/// (shape-only counters), `/_evoxy/admin/directives` (token-gated runtime "act"),
/// and `/_evoxy/explain/<target>` (shape-only routing dry-run). Returns the
/// immediate response for one of those, or `None` for a normal data-plane request.
async fn reserved_response<R: Router>(
    filter: &Filter<R>,
    metrics: &Metrics,
    directives: &Directives,
    admin_token: Option<&str>,
    headers: &[(String, String)],
) -> Option<ProcessingResponse> {
    if reserved_path(headers) == METRICS_PATH {
        return Some(metrics_response(metrics));
    }
    if reserved_path(headers) == ADMIN_PATH {
        return Some(admin_response(directives, admin_token, headers));
    }
    if let Some(target) = explain_target(headers) {
        let req = convert::filter_request(with_path(headers.to_vec(), &target), Vec::new());
        return Some(explain_response(filter.explain(&req).await));
    }
    None
}

/// The shape-only routing-decision observability header (M7).
const DECISION_HEADER: &str = "x-evoxy-decision";

/// Build the response-headers-phase reply: add the shape-only decision header when
/// the request resolved, else continue unchanged.
fn response_headers(decision: Option<String>) -> ProcessingResponse {
    let common = match decision {
        Some(shape) => extproc::CommonResponse {
            header_mutation: Some(extproc::HeaderMutation {
                set_headers: vec![HeaderValueOption {
                    header: Some(HeaderValue {
                        key: DECISION_HEADER.to_owned(),
                        // Envoy applies the byte `raw_value`, not the string `value`.
                        value: String::new(),
                        raw_value: shape.into_bytes(),
                    }),
                    // OVERWRITE_IF_EXISTS_OR_ADD.
                    append_action: 2,
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        },
        None => extproc::CommonResponse::default(),
    };
    wrap(Resp::ResponseHeaders(HeadersResponse {
        response: Some(common),
    }))
}

/// Build the response-body-phase reply: replace the body when it was reshaped,
/// else continue with it unchanged. A reshaped body changes length, so drop the
/// upstream `content-length` (Envoy recomputes it, else it rejects the mismatch).
fn response_body(shaped: Option<Vec<u8>>) -> ProcessingResponse {
    let (body_mutation, header_mutation) = match shaped {
        Some(body) => (
            Some(extproc::BodyMutation {
                mutation: Some(extproc::body_mutation::Mutation::Body(body)),
            }),
            Some(extproc::HeaderMutation {
                remove_headers: vec!["content-length".to_owned()],
                ..Default::default()
            }),
        ),
        None => (None, None),
    };
    let common = extproc::CommonResponse {
        body_mutation,
        header_mutation,
        ..Default::default()
    };
    wrap(Resp::ResponseBody(BodyResponse {
        response: Some(common),
    }))
}

/// A fail-closed `413` for a request body over the cap. Shape-only body (an error
/// code, no tenant values), matching the brain's immediate replies.
fn payload_too_large() -> ProcessingResponse {
    wrap(Resp::ImmediateResponse(ImmediateResponse {
        status: Some(HttpStatus { code: 413 }),
        body: br#"{"error":"payload_too_large"}"#.to_vec(),
        ..Default::default()
    }))
}

/// Which request phase a response is for (they carry the same `CommonResponse`
/// but in different wrappers).
enum Phase {
    Headers,
    Body,
}

/// Run the brain and wrap its effects in the phase-appropriate response, tallying
/// the outcome (routed vs. fail-closed) for `/metrics`.
async fn finalize<R: Router>(
    filter: &Filter<R>,
    metrics: &Metrics,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    phase: Phase,
) -> ProcessingResponse {
    let req = convert::filter_request(headers, body);
    let orig_method = req.method.clone();
    let orig_path = req.path().to_owned();
    let mut actions = ExtProcActions::default();
    let _decision = filter.handle(&req, &mut actions).await;
    match actions.finish(&orig_method, &orig_path) {
        Ok(common) => {
            metrics.record_routed();
            wrap(match phase {
                Phase::Headers => Resp::RequestHeaders(HeadersResponse {
                    response: Some(common),
                }),
                Phase::Body => Resp::RequestBody(BodyResponse {
                    response: Some(common),
                }),
            })
        }
        Err(immediate) => {
            metrics.record_rejected();
            wrap(Resp::ImmediateResponse(immediate))
        }
    }
}

/// Dispatch a write phase: async mode (ADR-010) produces the transformed request
/// durably and answers `202`; otherwise the normal transform-and-forward `finalize`.
async fn dispatch_write<R: Router>(
    filter: &Filter<R>,
    metrics: &Metrics,
    async_sink: Option<&dyn AsyncWriteSink>,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    phase: Phase,
) -> ProcessingResponse {
    if asyncwrite::wants_async(&headers) {
        finalize_async(filter, metrics, async_sink, headers, body).await
    } else {
        finalize(filter, metrics, headers, body, phase).await
    }
}

/// The async-write path (ADR-010): run the brain to get the physical request, then
/// produce it durably and answer `202` instead of forwarding. Refuses (`503`/`400`)
/// rather than lie when it cannot honor the async contract: no write endpoint, no
/// sink configured, or an unacknowledged produce.
async fn finalize_async<R: Router>(
    filter: &Filter<R>,
    metrics: &Metrics,
    async_sink: Option<&dyn AsyncWriteSink>,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
) -> ProcessingResponse {
    let req = convert::filter_request(headers, body);
    // Async mode is meaningful only for writes; a read cannot be `202`-queued.
    if !filter.is_write(&req) {
        metrics.record_rejected();
        return asyncwrite::read_unsupported();
    }
    // No sink configured: refuse rather than silently fall back to a sync write.
    let Some(sink) = async_sink else {
        metrics.record_rejected();
        return asyncwrite::async_unavailable();
    };

    let orig_method = req.method.clone();
    let orig_path = req.path().to_owned();
    let mut actions = ExtProcActions::default();
    let _decision = filter.handle(&req, &mut actions).await;
    // A fail-closed transform (unresolved/rejected) never becomes an accepted write.
    let (_method, path, body) = match actions.transformed(&orig_method, &orig_path) {
        Ok(parts) => parts,
        Err(immediate) => {
            metrics.record_rejected();
            return wrap(Resp::ImmediateResponse(immediate));
        }
    };

    // The broker must acknowledge before we answer `202`; otherwise refuse with
    // `503`, never a false `202`.
    if sink.produce_acked(&path, &body).await.is_ok() {
        metrics.record_routed();
        asyncwrite::accepted(&asyncwrite::op_id(&path, &body))
    } else {
        metrics.record_rejected();
        asyncwrite::fanout_unavailable()
    }
}

/// The request `:path` (query stripped), for the reserved-path check.
fn reserved_path(headers: &[(String, String)]) -> &str {
    headers
        .iter()
        .find(|(k, _)| k == ":path")
        .map_or("", |(_, v)| v.split('?').next().unwrap_or(""))
}

/// The `/metrics` reply: a shape-only snapshot as a `200` immediate response,
/// served by the filter itself (no second server, rides Envoy's port).
fn metrics_response(metrics: &Metrics) -> ProcessingResponse {
    wrap(Resp::ImmediateResponse(ImmediateResponse {
        status: Some(HttpStatus { code: 200 }),
        body: metrics.snapshot_json(),
        ..Default::default()
    }))
}

/// The reserved explain prefix: `/_evoxy/explain/<target path>` explains how
/// `<target path>` would route.
const EXPLAIN_PREFIX: &str = "/_evoxy/explain";

/// The target path an explain request names, or `None` if this is not an explain
/// request. `/_evoxy/explain/orders/_search` → `/orders/_search`.
fn explain_target(headers: &[(String, String)]) -> Option<String> {
    reserved_path(headers)
        .strip_prefix(EXPLAIN_PREFIX)
        .filter(|rest| rest.starts_with('/'))
        .map(str::to_owned)
}

/// The headers with `:path` replaced by `path` (to explain a different request).
fn with_path(mut headers: Vec<(String, String)>, path: &str) -> Vec<(String, String)> {
    for (key, value) in &mut headers {
        if key == ":path" {
            path.clone_into(value);
        }
    }
    headers
}

/// The `/explain` reply: the shape-only routing dry-run as a `200` immediate
/// response.
fn explain_response(body: String) -> ProcessingResponse {
    wrap(Resp::ImmediateResponse(ImmediateResponse {
        status: Some(HttpStatus { code: 200 }),
        body: body.into_bytes(),
        ..Default::default()
    }))
}

/// The token-gated directive-plane reply (M7 "act"): apply any directives named in
/// the query, then return the current shape-only snapshot. Requires
/// `Authorization: Bearer <token>` matching the configured admin token; without a
/// configured token, or on a mismatch, it fails closed `403` — the plane is off
/// unless deliberately enabled and correctly authenticated.
fn admin_response(
    directives: &Directives,
    admin_token: Option<&str>,
    headers: &[(String, String)],
) -> ProcessingResponse {
    let authorized = admin_token.is_some_and(|token| {
        bearer(headers).is_some_and(|got| constant_time_eq(got.as_bytes(), token.as_bytes()))
    });
    if !authorized {
        return admin_error(403, "unauthorized");
    }
    if let Some(query) = raw_query(headers) {
        directives.apply_query(query);
    }
    wrap(Resp::ImmediateResponse(ImmediateResponse {
        status: Some(HttpStatus { code: 200 }),
        body: directives.snapshot_json(),
        ..Default::default()
    }))
}

/// A shape-only fail-closed admin reply.
fn admin_error(status: u16, code: &str) -> ProcessingResponse {
    wrap(Resp::ImmediateResponse(ImmediateResponse {
        status: Some(HttpStatus {
            code: i32::from(status),
        }),
        body: format!("{{\"error\":\"{code}\"}}").into_bytes(),
        ..Default::default()
    }))
}

/// The bearer token from `Authorization: Bearer <token>` (case-insensitive scheme).
fn bearer(headers: &[(String, String)]) -> Option<&str> {
    let auth = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
        .map(|(_, v)| v.as_str())?;
    let (scheme, token) = auth.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then_some(token.trim())
}

/// The raw `?query` of the request `:path` (unlike [`reserved_path`], which strips
/// it), for the directive plane's query settings.
fn raw_query(headers: &[(String, String)]) -> Option<&str> {
    headers
        .iter()
        .find(|(k, _)| k == ":path")
        .and_then(|(_, v)| v.split_once('?'))
        .map(|(_, query)| query)
}

/// Wrap a response oneof into a `ProcessingResponse`.
pub(crate) fn wrap(response: Resp) -> ProcessingResponse {
    ProcessingResponse {
        response: Some(response),
        ..Default::default()
    }
}

#[cfg(test)]
#[path = "extproc_tests.rs"]
mod extproc_tests;
