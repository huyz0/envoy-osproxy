//! Transform-then-forward: turn a `RequestCtx` into the mutated request Envoy
//! forwards, or a fail-closed immediate response (ADR-002).
//!
//! This is the code-side of ADR-002. Given the user's tenancy (wrapped in a
//! [`Router`]) and the [`RequestCtx`] the adapter built, [`prepare`]:
//!
//! 1. resolves the request through the routing SPI (`Router::resolve`), reusing
//!    the osproxy engine's partition + placement + transform derivation;
//! 2. applies the resulting `BodyTransform` over the shared `osproxy-rewrite`
//!    byte-splice primitives (the `transform` module);
//! 3. produces a [`PreparedForward`] — the upstream cluster, physical path,
//!    constructed/`_id`, mutated body, and header ops — for the filter to hand
//!    back to Envoy.
//!
//! It **never dispatches**: no `Sink`, no upstream client. Envoy forwards. The
//! only responses it produces itself are fail-closed ([`Forward::Immediate`]):
//! an unresolved partition, a missing placement, a malformed body, or an
//! endpoint not yet supported. Bodies are shape-only (an error code, never a
//! tenant value).
#![deny(missing_docs)]

mod transform;

use evoxy_abi::FilterResponse;
use osproxy_core::EndpointKind;
use osproxy_rewrite::RewriteError;
use osproxy_spi::{RequestCtx, SpiError};
use osproxy_tenancy::Router;

/// What to do with a request after routing: forward it upstream (mutated) or
/// reply immediately (fail-closed).
#[derive(Debug, Clone)]
pub enum Forward {
    /// Forward upstream. Envoy selects the cluster and sends this mutated request.
    Upstream(PreparedForward),
    /// Reply now without forwarding (fail-closed).
    Immediate(FilterResponse),
}

/// The mutated request Envoy should forward: which upstream cluster, and the
/// rewritten request line + body. The filter maps `cluster` to an Envoy upstream
/// cluster (the ADR-002 `Target → cluster` seam) and applies `header_ops`.
#[derive(Debug, Clone)]
pub struct PreparedForward {
    /// The logical `ClusterId` (as a string) to route to; maps to an Envoy cluster.
    pub cluster: String,
    /// The HTTP method to forward with (`PUT` when a doc id is known, else `POST`).
    pub method: &'static str,
    /// The rewritten path (physical index, id, and `?routing=` when set).
    pub path: String,
    /// The mutated request body to forward.
    pub body: Vec<u8>,
    /// Header mutations to apply before forwarding (empty until migration/M5).
    pub header_ops: Vec<osproxy_spi::HeaderOp>,
}

/// Errors from applying the body transform. Kept separate from [`SpiError`] so
/// the two map to distinct fail-closed statuses.
#[derive(Debug, thiserror::Error)]
pub enum PrepareError {
    /// The body could not be transformed (not an object, reserved-field
    /// collision, un-expandable id template, …).
    #[error("body rewrite failed: {0}")]
    Rewrite(#[from] RewriteError),
    /// A context-derived injected value reached the transform unresolved — the
    /// router should have resolved it, so this is an internal invariant break.
    #[error("injected value reached the transform unresolved")]
    UnresolvedInjectedValue,
}

/// Prepare a request for forwarding. Dispatches by endpoint; M1 handles
/// single-document ingest, other endpoints are a fail-closed `501` until their
/// milestone lands.
pub async fn prepare<R: Router + ?Sized>(router: &R, ctx: &RequestCtx<'_>) -> Forward {
    match ctx.endpoint() {
        EndpointKind::IngestDoc => prepare_write(router, ctx).await,
        _ => Forward::Immediate(immediate(501, "endpoint_not_supported_yet")),
    }
}

/// The single-document write path: resolve, transform, and build the forward.
async fn prepare_write<R: Router + ?Sized>(router: &R, ctx: &RequestCtx<'_>) -> Forward {
    let resolved = match router.resolve(ctx).await {
        Ok(resolved) => resolved,
        Err(err) => return Forward::Immediate(immediate(spi_status(&err), spi_code(&err))),
    };

    let transformed = match transform::apply(
        ctx.body(),
        &resolved.decision.body_transform,
        resolved.partition.as_str(),
    ) {
        Ok(t) => t,
        Err(err) => return Forward::Immediate(immediate(prepare_status(&err), prepare_code(&err))),
    };

    let target = &resolved.decision.target;
    // A constructed id wins; otherwise the client's path id is the physical id
    // (dedicated placements keep the client id, SharedIndex always constructs).
    let physical_id = transformed.id.as_deref().or_else(|| ctx.doc_id());
    let (method, path) = write_path(
        target.index.as_str(),
        physical_id,
        transformed.routing.as_deref(),
    );

    Forward::Upstream(PreparedForward {
        cluster: target.cluster.as_str().to_owned(),
        method,
        path,
        body: transformed.body,
        header_ops: resolved.decision.header_ops.clone(),
    })
}

/// Build the physical write request line: `PUT /{index}/_doc/{id}` when the id is
/// known, else `POST /{index}/_doc`, appending `?routing=` when set.
fn write_path(index: &str, id: Option<&str>, routing: Option<&str>) -> (&'static str, String) {
    let (method, mut path) = match id {
        Some(id) => ("PUT", format!("/{index}/_doc/{id}")),
        None => ("POST", format!("/{index}/_doc")),
    };
    if let Some(routing) = routing {
        path.push_str("?routing=");
        path.push_str(routing);
    }
    (method, path)
}

/// A shape-only fail-closed response: `{"error":"<code>"}`, no tenant values.
fn immediate(status: u16, code: &str) -> FilterResponse {
    FilterResponse::json(status, format!("{{\"error\":\"{code}\"}}").into_bytes())
}

/// Map a routing `SpiError` to a fail-closed HTTP status.
fn spi_status(err: &SpiError) -> u16 {
    match err {
        SpiError::UnsupportedEndpoint { .. } => 501,
        SpiError::PlacementMissing { .. } | SpiError::PlacementBackend { .. } => 503,
        SpiError::IdRuleMissingPartition => 500,
        // PartitionUnresolved / PrincipalAttrMissing / HeaderMissing — and any
        // future variant — fail closed as a bad request rather than route blind.
        _ => 400,
    }
}

/// A stable, shape-only error code for a routing `SpiError` (no values).
fn spi_code(err: &SpiError) -> &'static str {
    match err {
        SpiError::PartitionUnresolved { .. } => "partition_unresolved",
        SpiError::PrincipalAttrMissing { .. } => "principal_attr_missing",
        SpiError::HeaderMissing { .. } => "header_missing",
        SpiError::UnsupportedEndpoint { .. } => "unsupported_endpoint",
        SpiError::PlacementMissing { .. } => "placement_missing",
        SpiError::PlacementBackend { .. } => "placement_backend",
        SpiError::IdRuleMissingPartition => "id_rule_missing_partition",
        _ => "routing_error",
    }
}

/// Map a [`PrepareError`] to a fail-closed HTTP status.
fn prepare_status(err: &PrepareError) -> u16 {
    match err {
        PrepareError::Rewrite(_) => 400,
        PrepareError::UnresolvedInjectedValue => 500,
    }
}

/// A stable, shape-only error code for a [`PrepareError`].
fn prepare_code(err: &PrepareError) -> &'static str {
    match err {
        PrepareError::Rewrite(_) => "body_rewrite_failed",
        PrepareError::UnresolvedInjectedValue => "unresolved_injected_value",
    }
}

#[cfg(test)]
#[path = "route_tests.rs"]
mod route_tests;
