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

mod observe;
mod reference;

pub use observe::{
    constant_time_eq, Directives, ImmediateReply, Metrics, Observe, ObserveConfig, ADMIN_PATH,
    DECISION_HEADER, EXPLAIN_PREFIX, METRICS_PATH,
};
pub use osproxy_spi::MigrationPhase;
pub use reference::{FilterConfig, Isolation, ReferenceTenancy};

use std::collections::BTreeSet;

use evoxy_abi::FilterRequest;
use evoxy_route::{prepare, Forward};
use osproxy_spi::HeaderOp;
use osproxy_tenancy::{Router, TenancyRouter};

/// The request header the filter sets to name the upstream cluster its placement
/// chose. Envoy's route config matches on it to pick the real cluster (the ADR-002
/// `Target → cluster` seam), so a tenancy that returns a different cluster per
/// request routes to a different upstream. Both backends use this one name; the
/// Envoy bootstrap must have header-matched routes for it (see the multi-cluster
/// example config) — without them the header is inert and every request falls
/// through to the default route.
pub const CLUSTER_HEADER: &str = "x-evoxy-cluster";

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
    /// Set the upstream host authority (`host:port`) the placement's endpoint names.
    /// The backend sets it as the request `:authority` so Envoy's
    /// dynamic-forward-proxy dials that host — the tenancy chooses the upstream by
    /// address, no cluster defined for it. A backend using static-cluster routing
    /// (by [`set_upstream_cluster`](Self::set_upstream_cluster)) can ignore this.
    fn set_upstream_host(&mut self, host: &str);
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
    /// Logical indices that bypass tenancy: a request for one is forwarded
    /// unchanged (no partition, no transform), to Envoy's default route. For
    /// global/shared indices that need no isolation. Empty by default.
    passthrough_indices: BTreeSet<String>,
    /// When set, the tenant is the first path segment: the filter strips it and
    /// puts it in this header before classifying, so a header-keyed tenancy resolves
    /// it. `None` disables path partitioning (the default).
    path_partition_header: Option<String>,
}

impl<R: Router> Filter<R> {
    /// Construct a filter over a resolved router (a `TenancyRouter` wrapping the
    /// user's `TenancySpi`).
    pub fn new(router: R) -> Self {
        Self {
            router,
            require_mtls_for_mutation: false,
            passthrough_indices: BTreeSet::new(),
            path_partition_header: None,
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

    /// Forward requests for these logical indices unchanged, bypassing tenancy: no
    /// partition, no transform, no cluster override. For global or shared indices
    /// that need no isolation.
    #[must_use]
    pub fn with_passthrough_indices(mut self, indices: impl IntoIterator<Item = String>) -> Self {
        self.passthrough_indices = indices.into_iter().collect();
        self
    }

    /// Take the tenant from the first path segment, moving it into `header` before
    /// classification (so `/acme/orders/_doc/1` routes as tenant `acme`, path
    /// `/orders/_doc/1`). Pair with a header-keyed tenancy reading the same header.
    #[must_use]
    pub fn with_path_partition_header(mut self, header: impl Into<String>) -> Self {
        self.path_partition_header = Some(header.into());
        self
    }

    /// If path partitioning is on, strip the first path segment into the configured
    /// header and return the rewritten request; else `None` (use the original).
    fn rewrite_for_path(&self, req: &FilterRequest) -> Option<FilterRequest> {
        let header = self.path_partition_header.as_ref()?;
        let (tenant, rest) = split_leading_segment(&req.path_and_query)?;
        let mut rewritten = req.clone();
        rewritten.path_and_query = rest;
        rewritten.headers.push((header.clone(), tenant));
        Some(rewritten)
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
        // Path partitioning (optional): the tenant is the first path segment, moved
        // into a header before classification.
        let rewritten = self.rewrite_for_path(req);
        let req = rewritten.as_ref().unwrap_or(req);

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

        // Passthrough: a global/shared index bypasses tenancy and is forwarded
        // unchanged (only the path-partition rewrite, if any, is applied).
        if self
            .passthrough_indices
            .contains(parts.ctx().logical_index())
        {
            if rewritten.is_some() {
                actions.set_path(&req.path_and_query);
            }
            return FilterDecision::ContinueUpstream;
        }

        apply_forward(prepare(&self.router, &parts.ctx()).await, actions)
    }

    /// The **header-phase routing** decision (M2c): resolve only the upstream
    /// cluster (from the request headers) and set it, so Envoy routes on
    /// `x-evoxy-cluster` before the body arrives. The body/path transform is
    /// applied later by [`Filter::handle`] at the body phase. A building block for
    /// a backend that routes by header at the header phase (multi-cluster ext_proc
    /// re-routing, ADR-002/M2c). The shipped backends do not use it: the single
    /// upstream reference tenancy routes statically, so both run the whole
    /// transform through [`Filter::handle`] instead (see the ext_proc
    /// `clear_route_cache: false` note in `evoxy-extproc`).
    pub async fn route_headers(
        &self,
        req: &FilterRequest,
        actions: &mut dyn EnvoyActions,
    ) -> FilterDecision {
        let rewritten = self.rewrite_for_path(req);
        let req = rewritten.as_ref().unwrap_or(req);
        let request_id = req.header("x-request-id").unwrap_or("");
        let parts = match evoxy_adapter::RequestParts::from_filter(req, request_id) {
            Ok(parts) => parts,
            Err(err) => {
                let body = format!("{{\"error\":\"{}\"}}", adapt_code(&err)).into_bytes();
                actions.send_local_reply(400, &json_headers(), &body);
                return FilterDecision::StoppedWithLocalReply;
            }
        };

        // A passthrough index needs no cluster resolution; forward unchanged.
        if self
            .passthrough_indices
            .contains(parts.ctx().logical_index())
        {
            if rewritten.is_some() {
                actions.set_path(&req.path_and_query);
            }
            return FilterDecision::ContinueUpstream;
        }

        match evoxy_route::resolve_target(&self.router, &parts.ctx()).await {
            Ok((cluster, host)) => {
                actions.set_upstream_cluster(&cluster);
                if let Some(host) = host {
                    actions.set_upstream_host(&host);
                }
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

    /// Whether `req` targets a write endpoint (a mutation). Used by a backend that
    /// gates a write-only feature (the ext_proc async-write mode) before running the
    /// transform. An unclassifiable request (unsupported method) is not a write.
    #[must_use]
    pub fn is_write(&self, req: &FilterRequest) -> bool {
        evoxy_adapter::RequestParts::from_filter(req, "")
            .is_ok_and(|parts| parts.ctx().endpoint().is_write())
    }

    /// A **shape-only** summary of the routing decision for `req` — the transform
    /// kind, migration phase, and isolation flag (no tenant values), for the
    /// backend to surface as an observability signal (M7). `None` if the request
    /// does not resolve (it then carries no decision to report).
    pub async fn decision_shape(&self, req: &FilterRequest) -> Option<String> {
        let parts = evoxy_adapter::RequestParts::from_filter(req, "").ok()?;
        let resolved = self.router.resolve(&parts.ctx()).await.ok()?;
        let mut shape = evoxy_route::decision_shape(&resolved);
        // Correlate with Envoy's span (M7): append the W3C trace-id when present.
        if let Some(trace) = req.trace_id() {
            shape.push_str(";trace=");
            shape.push_str(trace);
        }
        Some(shape)
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

/// Build a configured reference-tenancy filter from a parsed [`FilterConfig`]: the
/// reference tenancy plus the filter-level options it enables (passthrough indices,
/// path partitioning). This is the default artifact's whole brain.
#[must_use]
pub fn reference_filter(config: &FilterConfig) -> Filter<TenancyRouter<ReferenceTenancy>> {
    let mut filter = Filter::new(TenancyRouter::new(ReferenceTenancy::from_config(config)));
    if !config.passthrough_indices.is_empty() {
        filter = filter.with_passthrough_indices(config.passthrough_indices.iter().cloned());
    }
    if config.partition_from_path {
        filter = filter.with_path_partition_header(config.partition_header.clone());
    }
    filter
}

/// Split `/tenant/rest...` into (`tenant`, `/rest...`), preserving any `?query`.
/// `None` when there is no leading segment plus a remainder to route on.
fn split_leading_segment(path_and_query: &str) -> Option<(String, String)> {
    let (path, query) = match path_and_query.split_once('?') {
        Some((path, query)) => (path, Some(query)),
        None => (path_and_query, None),
    };
    let (tenant, rest) = path.trim_start_matches('/').split_once('/')?;
    if tenant.is_empty() {
        return None;
    }
    let mut rewritten = format!("/{rest}");
    if let Some(query) = query {
        rewritten.push('?');
        rewritten.push_str(query);
    }
    Some((tenant.to_owned(), rewritten))
}

/// Issue the effects for a routed [`Forward`]: mutate the request upstream, or send
/// the fail-closed immediate reply.
fn apply_forward(forward: Forward, actions: &mut dyn EnvoyActions) -> FilterDecision {
    match forward {
        Forward::Upstream(f) => {
            actions.set_upstream_cluster(&f.cluster);
            if let Some(host) = &f.upstream_host {
                actions.set_upstream_host(host);
            }
            actions.set_method(f.method);
            actions.set_path(&f.path);
            actions.set_body(&f.body);
            apply_header_ops(&f.header_ops, actions);
            FilterDecision::ContinueUpstream
        }
        Forward::Immediate(resp) => {
            actions.send_local_reply(resp.status, &resp.headers, &resp.body);
            FilterDecision::StoppedWithLocalReply
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
