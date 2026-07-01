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
// JUSTIFY: the transform-then-forward dispatch hub — `prepare` plus the per-
// endpoint forward builders, the read/response seams, and the fail-closed status
// mapping. Each family already lives in its own module (bulk/demux/read/response/
// transform); what remains here is the single dispatch narrative, which reads
// worse split across files than kept together.

mod bulk;
mod demux;
mod encode;
mod read;
mod response;
mod transform;

pub use response::{
    shape_bulk_response, shape_get_response, shape_mget_response, shape_msearch_response,
    shape_search_response,
};

use evoxy_abi::FilterResponse;
use osproxy_core::EndpointKind;
use osproxy_rewrite::RewriteError;
use osproxy_spi::{BodyTransform, RequestCtx, SpiError};
use osproxy_tenancy::{Resolved, Router};

/// A **shape-only** summary of a routing decision — the "why did this route here"
/// the extension knows but Envoy cannot (docs/00 §5): the transform kind, the
/// migration phase, and whether read/write isolation was applied. Deliberately
/// carries no tenant *values* (no partition, index, or id), only kinds and flags,
/// so it is safe to surface on every response (the no-value-leak rule).
#[must_use]
pub fn decision_shape(resolved: &Resolved) -> String {
    let transform = match &resolved.decision.body_transform {
        BodyTransform::None => "none",
        BodyTransform::Inject(_) => "inject",
        BodyTransform::ConstructId(_) => "construct_id",
        BodyTransform::Both { .. } => "both",
    };
    // Isolation is "on" when the placement injects a partition-scoping field
    // (shared-index); a dedicated placement isolates by cluster/index instead.
    let isolation = matches!(
        resolved.decision.body_transform,
        BodyTransform::Inject(_) | BodyTransform::Both { .. }
    );
    format!(
        "transform={transform};migration={};isolation={}",
        resolved.migration.as_str(),
        if isolation { "on" } else { "off" }
    )
}

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

/// Prepare a request for forwarding. Dispatches by endpoint: single-doc ingest,
/// by-id read/delete, and search/count are handled; others are a fail-closed
/// `501` until their milestone lands. Resolution (partition + placement) is
/// reused from the engine for every handled endpoint.
pub async fn prepare<R: Router + ?Sized>(router: &R, ctx: &RequestCtx<'_>) -> Forward {
    // Reject unhandled endpoints before resolving (cheaper, and avoids resolving a
    // bulk body as a single doc).
    let kind = ctx.endpoint();
    if !is_supported(kind) {
        return Forward::Immediate(immediate(501, "endpoint_not_supported_yet"));
    }

    let resolved = match router.resolve(ctx).await {
        Ok(resolved) => resolved,
        Err(err) => return Forward::Immediate(immediate(spi_status(&err), spi_code(&err))),
    };

    // Migration write gate (M5, docs/06 §2): a write resolved against a placement
    // that is now in the cutover window (or superseded) is held — fail closed with
    // a retryable `409`, so the client re-resolves against the new placement.
    // Reads are never gated (they always resolve to a single placement). This is
    // in-model: the write is rejected, never dispatched.
    if kind.is_write()
        && !router
            .admit_write(&resolved.partition, resolved.decision.epoch)
            .await
    {
        return Forward::Immediate(immediate(409, "stale_epoch"));
    }

    match kind {
        EndpointKind::IngestDoc => write_forward(&resolved, ctx),
        EndpointKind::IngestBulk => bulk_forward(&resolved, ctx),
        EndpointKind::GetById | EndpointKind::DeleteById => by_id_forward(&resolved, ctx),
        EndpointKind::Search => query_forward(&resolved, ctx, "_search"),
        EndpointKind::Count => query_forward(&resolved, ctx, "_count"),
        EndpointKind::MultiGet => demux_forward(&resolved, ctx, DemuxKind::MultiGet),
        EndpointKind::MultiSearch => demux_forward(&resolved, ctx, DemuxKind::MultiSearch),
        // Unreachable given the guard above, but fail closed rather than panic.
        _ => Forward::Immediate(immediate(501, "endpoint_not_supported_yet")),
    }
}

/// Whether [`prepare`] handles this endpoint (else it fails closed `501`).
fn is_supported(kind: EndpointKind) -> bool {
    matches!(
        kind,
        EndpointKind::IngestDoc
            | EndpointKind::IngestBulk
            | EndpointKind::GetById
            | EndpointKind::DeleteById
            | EndpointKind::Search
            | EndpointKind::Count
            | EndpointKind::MultiGet
            | EndpointKind::MultiSearch
    )
}

/// A **shape-only** routing explain (M7): resolve `ctx` as [`prepare`] would and
/// report *what* it would do — the endpoint kind, the outcome (`route`/`reject`),
/// and either the decision shape or the fail-closed status/code — as JSON, without
/// forwarding. Carries only kinds, flags, and status codes (no tenant value), so
/// it is a safe break-glass "why did this route here" for an operator. Partition
/// resolution uses the headers (not the body), so it explains a header/principal-
/// keyed tenancy; a body-keyed one reports the unresolved reject, honestly.
pub async fn explain<R: Router + ?Sized>(router: &R, ctx: &RequestCtx<'_>) -> String {
    let kind = ctx.endpoint();
    if !is_supported(kind) {
        return reject_json(kind, 501, "endpoint_not_supported_yet");
    }
    let resolved = match router.resolve(ctx).await {
        Ok(resolved) => resolved,
        Err(err) => return reject_json(kind, spi_status(&err), spi_code(&err)),
    };
    if kind.is_write()
        && !router
            .admit_write(&resolved.partition, resolved.decision.epoch)
            .await
    {
        return reject_json(kind, 409, "stale_epoch");
    }
    format!(
        "{{\"endpoint\":\"{}\",\"outcome\":\"route\",\"decision\":\"{}\"}}",
        kind.as_str(),
        decision_shape(&resolved)
    )
}

/// A shape-only fail-closed explain line: the endpoint, the reject outcome, and
/// the status/code `prepare` would return.
fn reject_json(kind: EndpointKind, status: u16, code: &str) -> String {
    format!(
        "{{\"endpoint\":\"{}\",\"outcome\":\"reject\",\"status\":{status},\"code\":\"{code}\"}}",
        kind.as_str()
    )
}

/// The `_bulk` path: rewrite the NDJSON in place (per-item inject/construct-id/
/// index) and forward as one bulk request; the physical index is on each action
/// line, so it goes to the cluster-level `/_bulk`.
fn bulk_forward(resolved: &Resolved, ctx: &RequestCtx<'_>) -> Forward {
    let body = match bulk::rewrite_bulk(resolved, ctx.body()) {
        Ok(body) => body,
        Err(err) => return Forward::Immediate(immediate(prepare_status(&err), prepare_code(&err))),
    };
    Forward::Upstream(PreparedForward {
        cluster: resolved.decision.target.cluster.as_str().to_owned(),
        method: "POST",
        path: "/_bulk".to_owned(),
        body,
        header_ops: resolved.decision.header_ops.clone(),
    })
}

/// Which multi-operation read endpoint a [`demux_forward`] handles.
#[derive(Clone, Copy)]
enum DemuxKind {
    MultiGet,
    MultiSearch,
}

/// The `_mget`/`_msearch` path: rewrite every operation to the one resolved
/// placement (physical index + partition-scoped id / partition filter) and
/// forward as one cluster-level request. Response ids/indices are mapped back to
/// the logical view on the way out (`shape_read_response`).
fn demux_forward(resolved: &Resolved, ctx: &RequestCtx<'_>, kind: DemuxKind) -> Forward {
    let (body_result, verb) = match kind {
        DemuxKind::MultiGet => (demux::rewrite_mget(resolved, ctx.body()), "_mget"),
        DemuxKind::MultiSearch => (demux::rewrite_msearch(resolved, ctx.body()), "_msearch"),
    };
    let body = match body_result {
        Ok(body) => body,
        Err(err) => return Forward::Immediate(immediate(prepare_status(&err), prepare_code(&err))),
    };
    Forward::Upstream(PreparedForward {
        cluster: resolved.decision.target.cluster.as_str().to_owned(),
        method: "POST",
        path: format!("/{verb}"),
        body,
        header_ops: resolved.decision.header_ops.clone(),
    })
}

/// Resolve just the upstream cluster for a request — the header-phase routing
/// decision (M2c). The partition comes from the request headers (for a
/// header-keyed tenancy), so the cluster is known before the body arrives; the
/// filter sets it at the header phase so Envoy routes on `x-evoxy-cluster`, and
/// applies the body/path transform at the body phase. Returns the logical
/// `ClusterId`, or a fail-closed [`FilterResponse`] for an unhandled endpoint or
/// a resolution error.
///
/// # Errors
/// A [`FilterResponse`] (501 for an unsupported endpoint, or the mapped routing
/// status) that the filter should send as an immediate reply.
pub async fn resolve_cluster<R: Router + ?Sized>(
    router: &R,
    ctx: &RequestCtx<'_>,
) -> Result<String, FilterResponse> {
    if !matches!(
        ctx.endpoint(),
        EndpointKind::IngestDoc
            | EndpointKind::GetById
            | EndpointKind::DeleteById
            | EndpointKind::Search
            | EndpointKind::Count
    ) {
        return Err(immediate(501, "endpoint_not_supported_yet"));
    }
    match router.resolve(ctx).await {
        Ok(resolved) => Ok(resolved.decision.target.cluster.as_str().to_owned()),
        Err(err) => Err(immediate(spi_status(&err), spi_code(&err))),
    }
}

/// Reshape a read's upstream response into the client's logical view (M2b),
/// resolving the routing decision from the request context. Returns the shaped
/// body, or `None` when there is nothing to do (not a shapeable read, resolution
/// failed, or the body could not be parsed) — the filter then forwards the
/// upstream body unchanged.
pub async fn shape_read_response<R: Router + ?Sized>(
    router: &R,
    ctx: &RequestCtx<'_>,
    upstream_body: &[u8],
) -> Option<Vec<u8>> {
    let resolved = router.resolve(ctx).await.ok()?;
    match ctx.endpoint() {
        EndpointKind::GetById => {
            shape_get_response(&resolved, ctx.logical_index(), ctx.doc_id()?, upstream_body).ok()
        }
        EndpointKind::Search => {
            shape_search_response(&resolved, ctx.logical_index(), upstream_body).ok()
        }
        EndpointKind::IngestBulk => {
            shape_bulk_response(&resolved, ctx.logical_index(), upstream_body).ok()
        }
        EndpointKind::MultiGet => {
            shape_mget_response(&resolved, ctx.logical_index(), upstream_body).ok()
        }
        EndpointKind::MultiSearch => {
            shape_msearch_response(&resolved, ctx.logical_index(), upstream_body).ok()
        }
        _ => None,
    }
}

/// The single-document write path: apply the body transform and build the forward.
fn write_forward(resolved: &Resolved, ctx: &RequestCtx<'_>) -> Forward {
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

/// The by-id read/delete path: map the client's logical id to the physical id
/// (`SharedIndex` constructs a partition-scoped id; dedicated keeps the client
/// id) and forward with no body. Response-side field-strip/id-unmap is M2b.
fn by_id_forward(resolved: &Resolved, ctx: &RequestCtx<'_>) -> Forward {
    let logical_id = ctx.doc_id().unwrap_or_default();
    let (physical_id, routing) = match read::physical_id(
        &resolved.decision.body_transform,
        resolved.partition.as_str(),
        logical_id,
    ) {
        Ok(mapped) => mapped,
        Err(err) => return Forward::Immediate(immediate(prepare_status(&err), prepare_code(&err))),
    };

    let target = &resolved.decision.target;
    // Percent-encode the id segment so a slash-bearing id (a URI principal) stays
    // one path segment; OpenSearch decodes it back to the exact id.
    let mut path = format!(
        "/{}/_doc/{}",
        target.index.as_str(),
        encode::encode(&physical_id)
    );
    if let Some(routing) = routing {
        path.push_str("?routing=");
        path.push_str(&encode::encode(&routing));
    }

    Forward::Upstream(PreparedForward {
        cluster: target.cluster.as_str().to_owned(),
        method: method_str(ctx.method()),
        path,
        body: Vec::new(),
        header_ops: resolved.decision.header_ops.clone(),
    })
}

/// The search/count path: inject the mandatory partition filter into the query
/// (the read isolation boundary, ADR-006) and forward to the physical index.
fn query_forward(resolved: &Resolved, ctx: &RequestCtx<'_>, verb: &str) -> Forward {
    let filter = read::filter_terms(
        &resolved.decision.body_transform,
        resolved.partition.as_str(),
    );
    let body = match read::filtered_query(ctx.body(), &filter) {
        Ok(body) => body,
        Err(err) => return Forward::Immediate(immediate(prepare_status(&err), prepare_code(&err))),
    };

    let target = &resolved.decision.target;
    Forward::Upstream(PreparedForward {
        cluster: target.cluster.as_str().to_owned(),
        method: "POST",
        path: format!("/{}/{verb}", target.index.as_str()),
        body,
        header_ops: resolved.decision.header_ops.clone(),
    })
}

/// The forwarded HTTP method as a static string.
fn method_str(method: osproxy_spi::HttpMethod) -> &'static str {
    use osproxy_spi::HttpMethod;
    match method {
        HttpMethod::Put => "PUT",
        HttpMethod::Post => "POST",
        HttpMethod::Delete => "DELETE",
        HttpMethod::Head => "HEAD",
        // `Get` plus any future non-exhaustive variant: a by-id read defaults to
        // GET rather than panicking.
        _ => "GET",
    }
}

/// Build the physical write request line: `PUT /{index}/_doc/{id}` when the id is
/// known, else `POST /{index}/_doc`, appending `?routing=` when set.
fn write_path(index: &str, id: Option<&str>, routing: Option<&str>) -> (&'static str, String) {
    let (method, mut path) = match id {
        // The id is percent-encoded (a constructed id may embed a URI partition);
        // the index name has no reserved chars, so it is left as-is.
        Some(id) => ("PUT", format!("/{index}/_doc/{}", encode::encode(id))),
        None => ("POST", format!("/{index}/_doc")),
    };
    if let Some(routing) = routing {
        path.push_str("?routing=");
        path.push_str(&encode::encode(routing));
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
