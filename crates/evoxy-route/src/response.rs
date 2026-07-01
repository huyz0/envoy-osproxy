//! Response-side reshaping (M2b): the read-path inverse of the write transform.
//!
//! On the way back, a document must be returned in the client's *logical* view:
//! injected tenancy fields stripped from `_source`, the physical `_id` mapped back
//! to the logical id, and `_index` presented as the logical index. This mirrors
//! the engine's `read` shaping, reusing the `osproxy-rewrite` primitives; the
//! filter calls it on Envoy's response path once the routing decision (carried
//! from the request phase) is known.

use osproxy_core::FieldName;
use osproxy_rewrite::{map_physical_to_logical, strip_fields};
use osproxy_spi::{BodyTransform, DocIdRule};
use osproxy_tenancy::Resolved;
use serde_json::Value;

use crate::PrepareError;

/// What the response shaping needs from the routing decision: the injected field
/// names to strip, and the id rule to invert.
struct ResponseShape {
    inject_names: Vec<FieldName>,
    id_rule: Option<DocIdRule>,
}

fn shape_of(transform: &BodyTransform) -> ResponseShape {
    let inject_names = match transform {
        BodyTransform::Inject(fields) | BodyTransform::Both { inject: fields, .. } => {
            fields.iter().map(|field| field.name.clone()).collect()
        }
        BodyTransform::None | BodyTransform::ConstructId(_) => Vec::new(),
    };
    let id_rule = match transform {
        BodyTransform::ConstructId(rule) | BodyTransform::Both { id: rule, .. } => {
            Some(rule.clone())
        }
        BodyTransform::None | BodyTransform::Inject(_) => None,
    };
    ResponseShape {
        inject_names,
        id_rule,
    }
}

/// Reshape a get-by-id response into the client's logical view: present the
/// logical `_index`/`_id`, and strip injected tenancy fields from `_source`.
///
/// # Errors
/// [`PrepareError::Rewrite`] if the upstream body is not valid JSON.
pub fn shape_get_response(
    resolved: &Resolved,
    logical_index: &str,
    logical_id: &str,
    upstream_body: &[u8],
) -> Result<Vec<u8>, PrepareError> {
    let shape = shape_of(&resolved.decision.body_transform);
    let mut doc: Value = parse(upstream_body)?;
    if let Some(obj) = doc.as_object_mut() {
        obj.insert("_index".to_owned(), Value::String(logical_index.to_owned()));
        obj.insert("_id".to_owned(), Value::String(logical_id.to_owned()));
        if let Some(source) = obj.get_mut("_source") {
            strip_fields(source, &shape.inject_names);
        }
    }
    serialize(&doc)
}

/// Reshape a search response: for each hit, present the logical `_index`, map the
/// physical `_id` back to logical, and strip injected fields from `_source`.
///
/// # Errors
/// [`PrepareError::Rewrite`] if the upstream body is not valid JSON.
pub fn shape_search_response(
    resolved: &Resolved,
    logical_index: &str,
    upstream_body: &[u8],
) -> Result<Vec<u8>, PrepareError> {
    let shape = shape_of(&resolved.decision.body_transform);
    let partition = resolved.partition.as_str();
    let mut top: Value = parse(upstream_body)?;
    shape_hits(&mut top, &shape, logical_index, partition);
    serialize(&top)
}

/// Reshape the `hits.hits[]` of a single search response in place (each hit into
/// the client's logical view). A response with no hits array is left untouched.
fn shape_hits(response: &mut Value, shape: &ResponseShape, logical_index: &str, partition: &str) {
    if let Some(hits) = response
        .get_mut("hits")
        .and_then(|h| h.get_mut("hits"))
        .and_then(Value::as_array_mut)
    {
        for hit in hits.iter_mut() {
            shape_hit(hit, shape, logical_index, partition);
        }
    }
}

/// Reshape one search hit in place.
fn shape_hit(hit: &mut Value, shape: &ResponseShape, logical_index: &str, partition: &str) {
    let Some(obj) = hit.as_object_mut() else {
        return;
    };
    obj.insert("_index".to_owned(), Value::String(logical_index.to_owned()));
    if let Some(rule) = &shape.id_rule {
        if let Some(Value::String(physical)) = obj.get("_id") {
            // Best-effort: an irreversible template leaves the physical id as-is.
            if let Ok(Some(logical)) =
                map_physical_to_logical(rule.template.as_str(), partition, physical)
            {
                obj.insert("_id".to_owned(), Value::String(logical));
            }
        }
    }
    if let Some(source) = obj.get_mut("_source") {
        strip_fields(source, &shape.inject_names);
    }
}

/// Reshape a `_bulk` response into the client's logical view: for each entry in
/// `items[]`, present the logical `_index` and map the physical `_id` back to the
/// logical id. Each item is a single-key object (`{"index":{...}}`,
/// `{"delete":{...}}`, …); the inner result object is reshaped in place.
///
/// # Errors
/// [`PrepareError::Rewrite`] if the upstream body is not valid JSON.
pub fn shape_bulk_response(
    resolved: &Resolved,
    logical_index: &str,
    upstream_body: &[u8],
) -> Result<Vec<u8>, PrepareError> {
    let shape = shape_of(&resolved.decision.body_transform);
    let partition = resolved.partition.as_str();
    let mut top: Value = parse(upstream_body)?;
    if let Some(items) = top.get_mut("items").and_then(Value::as_array_mut) {
        for item in items.iter_mut() {
            // Each item is a one-key object keyed by the verb; reshape the result.
            if let Some(result) = item.as_object_mut().and_then(|obj| obj.values_mut().next()) {
                shape_bulk_result(result, &shape, logical_index, partition);
            }
        }
    }
    serialize(&top)
}

/// Reshape one bulk item result in place (the object under the verb key).
fn shape_bulk_result(
    result: &mut Value,
    shape: &ResponseShape,
    logical_index: &str,
    partition: &str,
) {
    let Some(obj) = result.as_object_mut() else {
        return;
    };
    if obj.contains_key("_index") {
        obj.insert("_index".to_owned(), Value::String(logical_index.to_owned()));
    }
    if let Some(rule) = &shape.id_rule {
        if let Some(Value::String(physical)) = obj.get("_id") {
            // Best-effort: an irreversible template leaves the physical id as-is.
            if let Ok(Some(logical)) =
                map_physical_to_logical(rule.template.as_str(), partition, physical)
            {
                obj.insert("_id".to_owned(), Value::String(logical));
            }
        }
    }
}

/// Reshape an `_mget` response: each entry in `docs[]` is presented in the
/// client's logical view (logical `_index`, physical `_id` → logical, injected
/// fields stripped from `_source`) — the multi-get analog of `shape_get_response`.
///
/// # Errors
/// [`PrepareError::Rewrite`] if the upstream body is not valid JSON.
pub fn shape_mget_response(
    resolved: &Resolved,
    logical_index: &str,
    upstream_body: &[u8],
) -> Result<Vec<u8>, PrepareError> {
    let shape = shape_of(&resolved.decision.body_transform);
    let partition = resolved.partition.as_str();
    let mut top: Value = parse(upstream_body)?;
    if let Some(docs) = top.get_mut("docs").and_then(Value::as_array_mut) {
        for doc in docs.iter_mut() {
            // A fetched doc has the same shape as a search hit (`_index`/`_id`/
            // `_source`), so the per-hit reshape applies directly.
            shape_hit(doc, &shape, logical_index, partition);
        }
    }
    serialize(&top)
}

/// Reshape an `_msearch` response: each entry in `responses[]` is a full search
/// response whose `hits.hits[]` are reshaped into the client's logical view.
///
/// # Errors
/// [`PrepareError::Rewrite`] if the upstream body is not valid JSON.
pub fn shape_msearch_response(
    resolved: &Resolved,
    logical_index: &str,
    upstream_body: &[u8],
) -> Result<Vec<u8>, PrepareError> {
    let shape = shape_of(&resolved.decision.body_transform);
    let partition = resolved.partition.as_str();
    let mut top: Value = parse(upstream_body)?;
    if let Some(responses) = top.get_mut("responses").and_then(Value::as_array_mut) {
        for response in responses.iter_mut() {
            shape_hits(response, &shape, logical_index, partition);
        }
    }
    serialize(&top)
}

fn parse(body: &[u8]) -> Result<Value, PrepareError> {
    serde_json::from_slice(body)
        .map_err(|_| PrepareError::Rewrite(osproxy_rewrite::RewriteError::InvalidJson))
}

fn serialize(value: &Value) -> Result<Vec<u8>, PrepareError> {
    serde_json::to_vec(value)
        .map_err(|_| PrepareError::Rewrite(osproxy_rewrite::RewriteError::InvalidJson))
}
