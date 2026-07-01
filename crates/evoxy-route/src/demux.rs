//! `_mget` / `_msearch` request rewriting (M3): the multi-operation read demux
//! for the single-upstream model (ADR-002).
//!
//! Both endpoints carry many operations in one request. Under transform-then-
//! forward with a single upstream, every operation is rewritten to the one
//! resolved placement — `_mget` fetches get the physical index and a partition-
//! scoped id, `_msearch` header lines get the physical index and each query the
//! mandatory partition filter (the read isolation boundary, ADR-006) — then the
//! whole request is forwarded once. Cross-cluster fan-out (a per-item target) is
//! out of scope, exactly as in osproxy's single-target model. The partition is
//! resolved once from the request (a header-keyed tenancy applies it to every
//! item); per-operation partition resolution is a refinement.

use osproxy_rewrite::{parse_mget, parse_msearch};
use osproxy_tenancy::Resolved;
use serde_json::{json, Map, Value};

use crate::{read, PrepareError};

/// Rewrite an `_mget` body: each fetch targets the physical index with a
/// partition-scoped id (and `routing` when the id rule sets it). The response is
/// mapped back to the logical view on the way out (`shape_mget_response`).
pub(crate) fn rewrite_mget(resolved: &Resolved, body: &[u8]) -> Result<Vec<u8>, PrepareError> {
    let items = parse_mget(body)?;
    let partition = resolved.partition.as_str();
    let physical_index = resolved.decision.target.index.as_str();
    let transform = &resolved.decision.body_transform;

    let mut docs = Vec::with_capacity(items.len());
    for item in items {
        let (physical_id, routing) = read::physical_id(transform, partition, &item.id)?;
        let mut doc = Map::new();
        doc.insert("_index".to_owned(), json!(physical_index));
        doc.insert("_id".to_owned(), json!(physical_id));
        if let Some(routing) = routing {
            doc.insert("routing".to_owned(), json!(routing));
        }
        docs.push(Value::Object(doc));
    }
    serialize(&json!({ "docs": docs }))
}

/// Rewrite an `_msearch` NDJSON body: force every header line's index to the
/// physical index, and wrap each query with the partition filter.
pub(crate) fn rewrite_msearch(resolved: &Resolved, body: &[u8]) -> Result<Vec<u8>, PrepareError> {
    let items = parse_msearch(body)?;
    let partition = resolved.partition.as_str();
    let physical_index = resolved.decision.target.index.as_str();
    let filter = read::filter_terms(&resolved.decision.body_transform, partition);

    let mut out = Vec::new();
    for item in items {
        // Header line: pin the search to the physical index.
        let header = json!({ "index": physical_index });
        out.extend_from_slice(
            &serde_json::to_vec(&header)
                .map_err(|_| PrepareError::Rewrite(osproxy_rewrite::RewriteError::InvalidJson))?,
        );
        out.push(b'\n');
        // Query line: inject the mandatory partition filter (isolation).
        let query = read::filtered_query(&item.query, &filter)?;
        out.extend_from_slice(&query);
        out.push(b'\n');
    }
    Ok(out)
}

fn serialize(value: &Value) -> Result<Vec<u8>, PrepareError> {
    serde_json::to_vec(value)
        .map_err(|_| PrepareError::Rewrite(osproxy_rewrite::RewriteError::InvalidJson))
}
