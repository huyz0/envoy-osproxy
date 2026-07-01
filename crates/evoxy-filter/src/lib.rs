//! The filter brain — SDK-agnostic (ADR-004).
//!
//! [`Filter`] drives one request through the reused pipeline: build the
//! [`RequestCtx`](osproxy_spi::RequestCtx) via `evoxy-adapter`, resolve+transform
//! via `evoxy-route`, and issue the resulting effects through [`EnvoyActions`] —
//! our own abstraction of the Envoy filter callbacks. It has **no dependency on
//! the Envoy SDK**: the real ABI binding implements [`EnvoyActions`] over the SDK
//! handle in the workspace-excluded `evoxy-module` crate.
//!
//! This is what makes the brain testable without Envoy (a fake `EnvoyActions`
//! records the calls) and decoupled from the SDK's version/ABI.
#![deny(missing_docs)]

mod reference;

pub use reference::{FilterConfig, ReferenceTenancy};

use evoxy_abi::FilterRequest;
use evoxy_route::{prepare, Forward};
use osproxy_spi::HeaderOp;
use osproxy_tenancy::Router;

/// The Envoy-side effects the filter needs, abstracted from the SDK. The excluded
/// `evoxy-module` crate implements this over the real Envoy filter handle; tests
/// implement it with a recorder. All methods are synchronous `&mut self` so the
/// trait stays object-safe (`&mut dyn EnvoyActions`).
pub trait EnvoyActions {
    /// Route the request to this upstream cluster (the logical `ClusterId`; the
    /// Envoy bootstrap maps it to a real cluster — the ADR-002 seam).
    fn set_upstream_cluster(&mut self, cluster: &str);
    /// Replace the request method (`:method`).
    fn set_method(&mut self, method: &str);
    /// Replace the request path (`:path`).
    fn set_path(&mut self, path: &str);
    /// Replace the request body.
    fn set_body(&mut self, body: &[u8]);
    /// Add or replace a request header.
    fn set_header(&mut self, name: &str, value: &str);
    /// Remove a request header.
    fn remove_header(&mut self, name: &str);
    /// Stop the filter chain and reply to the client directly (fail-closed).
    fn send_local_reply(&mut self, status: u16, headers: &[(String, String)], body: &[u8]);
}

/// What the caller (the SDK glue) should tell Envoy after [`Filter::handle`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterDecision {
    /// The request was mutated in place; continue the filter chain so Envoy
    /// forwards it to the selected upstream cluster.
    ContinueUpstream,
    /// A local reply was sent (fail-closed); stop the filter chain.
    StoppedWithLocalReply,
}

/// The request-handling brain, generic over the resolved tenancy [`Router`].
#[derive(Debug, Clone)]
pub struct Filter<R> {
    router: R,
}

impl<R: Router> Filter<R> {
    /// Construct a filter over a resolved router (a `TenancyRouter` wrapping the
    /// user's `TenancySpi`).
    pub fn new(router: R) -> Self {
        Self { router }
    }

    /// Handle one request: adapt → resolve+transform → issue effects. Returns
    /// whether Envoy should continue upstream or stop (a local reply was sent).
    ///
    /// `async` because routing is async; the SDK glue drives it on a runtime
    /// (ADR-004). Never dispatches — Envoy forwards the mutated request.
    pub async fn handle(
        &self,
        req: &FilterRequest,
        actions: &mut dyn EnvoyActions,
    ) -> FilterDecision {
        // Carry Envoy's request id through for traceability (docs/09).
        let request_id = req.header("x-request-id").unwrap_or("");
        let parts = match evoxy_adapter::RequestParts::from_filter(req, request_id) {
            Ok(parts) => parts,
            Err(err) => {
                let body = format!("{{\"error\":\"{}\"}}", adapt_code(&err)).into_bytes();
                actions.send_local_reply(400, &json_headers(), &body);
                return FilterDecision::StoppedWithLocalReply;
            }
        };

        match prepare(&self.router, &parts.ctx()).await {
            Forward::Upstream(forward) => {
                actions.set_upstream_cluster(&forward.cluster);
                actions.set_method(forward.method);
                actions.set_path(&forward.path);
                actions.set_body(&forward.body);
                apply_header_ops(&forward.header_ops, actions);
                FilterDecision::ContinueUpstream
            }
            Forward::Immediate(resp) => {
                actions.send_local_reply(resp.status, &resp.headers, &resp.body);
                FilterDecision::StoppedWithLocalReply
            }
        }
    }
}

/// Apply the decision's header mutations to the forwarded request.
fn apply_header_ops(ops: &[HeaderOp], actions: &mut dyn EnvoyActions) {
    for op in ops {
        match op {
            HeaderOp::Add { name, value } | HeaderOp::Replace { name, value } => {
                actions.set_header(name, value);
            }
            HeaderOp::Remove { name } => actions.remove_header(name),
            // `HeaderOp` is non-exhaustive; a future op is ignored rather than
            // mis-applied (header ops are empty until migration/M5 anyway).
            _ => {}
        }
    }
}

/// The `content-type: application/json` header set for shape-only error replies.
fn json_headers() -> Vec<(String, String)> {
    vec![("content-type".to_owned(), "application/json".to_owned())]
}

/// A stable, shape-only code for an adapter error (only method is fallible today).
fn adapt_code(err: &evoxy_adapter::AdaptError) -> &'static str {
    match err {
        evoxy_adapter::AdaptError::UnsupportedMethod(_) => "unsupported_method",
    }
}

#[cfg(test)]
#[path = "filter_tests.rs"]
mod filter_tests;
