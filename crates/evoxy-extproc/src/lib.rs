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
mod service;

/// The generated Envoy ext_proc v3 types.
pub(crate) use envoy_types::pb::envoy::service::ext_proc::v3 as extproc;

pub use actions::CLUSTER_HEADER;
pub use service::{ExtProcService, ExternalProcessorServer};

use actions::ExtProcActions;
use evoxy_filter::Filter;
use extproc::processing_request::Request as Req;
use extproc::processing_response::Response as Resp;
use extproc::{BodyResponse, HeadersResponse, ProcessingRequest, ProcessingResponse};
use osproxy_tenancy::Router;

/// Per-stream state: the request headers, buffered from the headers phase so the
/// body phase can build the full request.
#[derive(Default)]
struct StreamState {
    headers: Vec<(String, String)>,
}

/// Process one ext_proc message, producing the response Envoy expects for that
/// phase. Headers are buffered; the request is resolved+mutated at the body phase
/// (a headerless request — a read — is resolved at the headers phase).
async fn process_message<R: Router>(
    filter: &Filter<R>,
    state: &mut StreamState,
    request: ProcessingRequest,
) -> ProcessingResponse {
    match request.request {
        Some(Req::RequestHeaders(headers)) => {
            state.headers = convert::extract_headers(&headers);
            if headers.end_of_stream {
                finalize(filter, state.headers.clone(), Vec::new(), Phase::Headers).await
            } else {
                // Continue; the mutation happens once we have the body. Envoy
                // requires a `CommonResponse` (an empty response is rejected).
                wrap(Resp::RequestHeaders(HeadersResponse {
                    response: Some(extproc::CommonResponse::default()),
                }))
            }
        }
        Some(Req::RequestBody(body)) => {
            finalize(filter, state.headers.clone(), body.body, Phase::Body).await
        }
        // M1 configures ext_proc for the request path only; other phases just
        // continue unmodified.
        _ => wrap(Resp::RequestBody(BodyResponse {
            response: Some(extproc::CommonResponse::default()),
        })),
    }
}

/// Which request phase a response is for (they carry the same `CommonResponse`
/// but in different wrappers).
enum Phase {
    Headers,
    Body,
}

/// Run the brain and wrap its effects in the phase-appropriate response.
async fn finalize<R: Router>(
    filter: &Filter<R>,
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
        Ok(common) => wrap(match phase {
            Phase::Headers => Resp::RequestHeaders(HeadersResponse {
                response: Some(common),
            }),
            Phase::Body => Resp::RequestBody(BodyResponse {
                response: Some(common),
            }),
        }),
        Err(immediate) => wrap(Resp::ImmediateResponse(immediate)),
    }
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
