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

pub use osproxy_spi::MigrationPhase;
pub use reference::{FilterConfig, ReferenceTenancy};

use evoxy_abi::FilterRequest;
use evoxy_route::{prepare, Forward};
use osproxy_spi::HeaderOp;
use osproxy_tenancy::Router;

/// The Envoy-side effects the filter needs, abstracted from the SDK. The excluded
/// `evoxy-module` crate implements this over the real Envoy filter handle; tests
/// implement it with a recorder. All methods are synchronous `&mut self` so the
/// trait stays object-safe (`&mut dyn EnvoyActions`). `Send` so a `dyn
/// EnvoyActions` can be held across an `await` in a spawned task (the ext_proc
/// backend streams responses from a task).
pub trait EnvoyActions: Send {
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
    require_mtls_for_mutation: bool,
}

impl<R: Router> Filter<R> {
    /// Construct a filter over a resolved router (a `TenancyRouter` wrapping the
    /// user's `TenancySpi`).
    pub fn new(router: R) -> Self {
        Self {
            router,
            require_mtls_for_mutation: false,
        }
    }

    /// Require an Envoy-validated mTLS identity for write endpoints (M4): a
    /// mutation (`EndpointKind::is_write`) with no presented client certificate is
    /// refused with a fail-closed `403`. Reads are unaffected. The identity comes
    /// from Envoy's XFCC header (see [`evoxy_abi::MtlsIdentity::from_xfcc`]).
    #[must_use]
    pub fn with_require_mtls_for_mutation(mut self, require: bool) -> Self {
        self.require_mtls_for_mutation = require;
        self
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

        // mTLS-for-mutation policy (M4): refuse a write without an Envoy-validated
        // client identity, before routing. Reads are unaffected.
        if self.require_mtls_for_mutation
            && parts.ctx().endpoint().is_write()
            && !req.identity.presented
        {
            let body = br#"{"error":"mtls_required_for_mutation"}"#.to_vec();
            actions.send_local_reply(403, &json_headers(), &body);
            return FilterDecision::StoppedWithLocalReply;
        }

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

    /// The **header-phase routing** decision (M2c): resolve only the upstream
    /// cluster (from the request headers) and set it, so Envoy routes on
    /// `x-evoxy-cluster` before the body arrives. The body/path transform is
    /// applied later by [`Filter::handle`] at the body phase. Used by backends
    /// where the routing header must be set at the header phase to take effect
    /// (both ext_proc's re-route and the dynamic module's header-only handle).
    pub async fn route_headers(
        &self,
        req: &FilterRequest,
        actions: &mut dyn EnvoyActions,
    ) -> FilterDecision {
        let request_id = req.header("x-request-id").unwrap_or("");
        let parts = match evoxy_adapter::RequestParts::from_filter(req, request_id) {
            Ok(parts) => parts,
            Err(err) => {
                let body = format!("{{\"error\":\"{}\"}}", adapt_code(&err)).into_bytes();
                actions.send_local_reply(400, &json_headers(), &body);
                return FilterDecision::StoppedWithLocalReply;
            }
        };

        match evoxy_route::resolve_cluster(&self.router, &parts.ctx()).await {
            Ok(cluster) => {
                actions.set_upstream_cluster(&cluster);
                FilterDecision::ContinueUpstream
            }
            Err(resp) => {
                actions.send_local_reply(resp.status, &resp.headers, &resp.body);
                FilterDecision::StoppedWithLocalReply
            }
        }
    }

    /// The **response phase** (M2b): reshape a read's upstream response into the
    /// client's logical view (strip injected fields, map physical ids back to
    /// logical). `req` is rebuilt from the request headers the backend buffered;
    /// `upstream_body` is the response body from the cluster. Returns the shaped
    /// body, or `None` when there is nothing to do (the backend then forwards the
    /// upstream body unchanged).
    pub async fn shape_response(
        &self,
        req: &FilterRequest,
        upstream_body: &[u8],
    ) -> Option<Vec<u8>> {
        let parts = evoxy_adapter::RequestParts::from_filter(req, "").ok()?;
        evoxy_route::shape_read_response(&self.router, &parts.ctx(), upstream_body).await
    }

    /// A **shape-only** summary of the routing decision for `req` — the transform
    /// kind, migration phase, and isolation flag (no tenant values), for the
    /// backend to surface as an observability signal (M7). `None` if the request
    /// does not resolve (it then carries no decision to report).
    pub async fn decision_shape(&self, req: &FilterRequest) -> Option<String> {
        let parts = evoxy_adapter::RequestParts::from_filter(req, "").ok()?;
        let resolved = self.router.resolve(&parts.ctx()).await.ok()?;
        Some(evoxy_route::decision_shape(&resolved))
    }

    /// A **shape-only** routing explain (M7) for `req` — what the filter *would*
    /// do (route with a decision shape, or the fail-closed status/code) without
    /// forwarding. The break-glass "why did this route here" for an operator,
    /// served by the backend on a reserved path.
    pub async fn explain(&self, req: &FilterRequest) -> String {
        match evoxy_adapter::RequestParts::from_filter(req, "") {
            Ok(parts) => evoxy_route::explain(&self.router, &parts.ctx()).await,
            Err(_) => r#"{"outcome":"reject","status":400,"code":"unsupported_method"}"#.to_owned(),
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
