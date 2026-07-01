//! `_bulk` NDJSON rewriting (M3): the request-side transform for bulk ingest.
//!
//! For the transform-then-forward model with a single upstream (ADR-002), a
//! `_bulk` request is rewritten **in place** — each action line's `_index` is set
//! to the physical index and its `_id` to the partition-scoped physical id, and
//! each source line has the isolation fields injected — then forwarded as one
//! bulk request. (Cross-cluster bulk demux would need fan-out, which Envoy's
//! single forward does not do; that is out of scope, as in osproxy's single-target
//! model.) The partition is resolved once from the request (a header-keyed tenancy
//! applies it to every item); per-document partition resolution is a refinement.

use osproxy_rewrite::{map_logical_to_physical, parse_bulk, BulkAction};
use osproxy_spi::{BodyTransform, DocIdRule};
use osproxy_tenancy::Resolved;
use serde_json::{json, Map, Value};

use crate::{transform, PrepareError};

/// Rewrite a bulk NDJSON body for `resolved`'s placement.
pub(crate) fn rewrite_bulk(resolved: &Resolved, body: &[u8]) -> Result<Vec<u8>, PrepareError> {
    let items = parse_bulk(body)?;
    let partition = resolved.partition.as_str();
    let physical_index = resolved.decision.target.index.as_str();
    let transform = &resolved.decision.body_transform;

    let mut out = Vec::new();
    for item in items {
        let (physical_id, source_out) = if let Some(source) = &item.source {
            // index/create/update: inject the isolation fields, and construct the
            // id from the source unless the action line carried an explicit id.
            let transformed = transform::apply(source, transform, partition)?;
            let id = match &item.id {
                Some(explicit) => Some(map_id(transform, partition, explicit)?),
                None => transformed.id,
            };
            (id, Some(transformed.body))
        } else {
            // delete: no source; map the client's id to the physical id.
            let id = match &item.id {
                Some(explicit) => Some(map_id(transform, partition, explicit)?),
                None => None,
            };
            (id, None)
        };

        write_action_line(
            &mut out,
            item.action,
            physical_index,
            physical_id.as_deref(),
            transform,
            partition,
        )?;
        if let Some(source) = source_out {
            out.extend_from_slice(&source);
            out.push(b'\n');
        }
    }
    Ok(out)
}

/// Emit one rewritten action line: `{"<verb>":{"_index":..,"_id":..,"routing":..}}`.
fn write_action_line(
    out: &mut Vec<u8>,
    action: BulkAction,
    physical_index: &str,
    physical_id: Option<&str>,
    transform: &BodyTransform,
    partition: &str,
) -> Result<(), PrepareError> {
    let mut meta = Map::new();
    meta.insert("_index".to_owned(), json!(physical_index));
    if let Some(id) = physical_id {
        meta.insert("_id".to_owned(), json!(id));
    }
    if let Some(routing) = routing_of(transform, partition) {
        meta.insert("routing".to_owned(), json!(routing));
    }
    let line = json!({ verb(action): Value::Object(meta) });
    let bytes = serde_json::to_vec(&line)
        .map_err(|_| PrepareError::Rewrite(osproxy_rewrite::RewriteError::InvalidJson))?;
    out.extend_from_slice(&bytes);
    out.push(b'\n');
    Ok(())
}

/// The id rule carried by the transform, if the placement constructs ids.
fn id_rule(transform: &BodyTransform) -> Option<&DocIdRule> {
    match transform {
        BodyTransform::ConstructId(rule) | BodyTransform::Both { id: rule, .. } => Some(rule),
        BodyTransform::None | BodyTransform::Inject(_) => None,
    }
}

/// Map a client's logical id to the physical id (partition-scoped when the
/// placement constructs ids, else unchanged).
fn map_id(
    transform: &BodyTransform,
    partition: &str,
    logical: &str,
) -> Result<String, PrepareError> {
    match id_rule(transform) {
        Some(rule) => Ok(map_logical_to_physical(
            rule.template.as_str(),
            partition,
            logical,
        )?),
        None => Ok(logical.to_owned()),
    }
}

/// The `routing` value: the partition, when the id rule set routing.
fn routing_of(transform: &BodyTransform, partition: &str) -> Option<String> {
    id_rule(transform)
        .filter(|rule| rule.set_routing)
        .map(|_| partition.to_owned())
}

/// The NDJSON verb for a bulk action.
fn verb(action: BulkAction) -> &'static str {
    match action {
        BulkAction::Index => "index",
        BulkAction::Create => "create",
        BulkAction::Update => "update",
        BulkAction::Delete => "delete",
    }
}
