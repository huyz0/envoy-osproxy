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

mod actions;
mod convert;
mod metrics;
mod service;

/// The generated Envoy ext_proc v3 types.
pub(crate) use envoy_types::pb::envoy::service::ext_proc::v3 as extproc;

pub use actions::CLUSTER_HEADER;
pub use service::{ExtProcService, ExternalProcessorServer};

use actions::ExtProcActions;
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
    state: &mut StreamState,
    request: ProcessingRequest,
) -> ProcessingResponse {
    match request.request {
        Some(Req::RequestHeaders(headers)) => {
            state.headers = convert::extract_headers(&headers);
            // Reserved admin paths (M7), answered by the filter itself and short-
            // circuited before any routing — not data-plane requests, so not
            // counted. `/_evoxy/metrics` is a shape-only counter snapshot;
            // `/_evoxy/explain/<target>` is a shape-only routing dry-run.
            if reserved_path(&state.headers) == METRICS_PATH {
                return metrics_response(metrics);
            }
            if let Some(target) = explain_target(&state.headers) {
                let req =
                    convert::filter_request(with_path(state.headers.clone(), &target), Vec::new());
                return explain_response(filter.explain(&req).await);
            }
            if headers.end_of_stream {
                finalize(
                    filter,
                    metrics,
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
            finalize(
                filter,
                metrics,
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
            // the "why did this route here" the extension knows and Envoy cannot.
            let req = convert::filter_request(state.headers.clone(), Vec::new());
            response_headers(filter.decision_shape(&req).await)
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

/// Wrap a response oneof into a `ProcessingResponse`.
fn wrap(response: Resp) -> ProcessingResponse {
    ProcessingResponse {
        response: Some(response),
        ..Default::default()
    }
}

#[cfg(test)]
#[path = "extproc_tests.rs"]
mod extproc_tests;
