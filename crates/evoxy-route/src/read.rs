//! Read-path request rewriting — the by-id logical→physical id mapping and the
//! search/count partition filter, reusing the `osproxy-rewrite` primitives.
//!
//! Response-side reshaping (strip injected fields, map physical ids back to
//! logical) happens on Envoy's response path and is M2b; here we only rewrite the
//! outgoing request, which is where read **isolation** is enforced (the mandatory
//! partition filter, ADR-006).

use osproxy_core::FieldName;
use osproxy_rewrite::{map_logical_to_physical, wrap_query};
use osproxy_spi::{BodyTransform, InjectedValue};
use serde_json::Value;

use crate::PrepareError;

/// Map a client's logical id to `(physical_id, routing)` for a by-id request. A
/// placement with an id rule constructs a partition-scoped physical id (and sets
/// `_routing` when the rule asks); otherwise the client id is already physical.
pub(crate) fn physical_id(
    transform: &BodyTransform,
    partition: &str,
    logical_id: &str,
) -> Result<(String, Option<String>), PrepareError> {
    let rule = match transform {
        BodyTransform::ConstructId(rule) | BodyTransform::Both { id: rule, .. } => Some(rule),
        BodyTransform::None | BodyTransform::Inject(_) => None,
    };
    match rule {
        Some(rule) => {
            let physical = map_logical_to_physical(rule.template.as_str(), partition, logical_id)?;
            let routing = rule.set_routing.then(|| partition.to_owned());
            Ok((physical, routing))
        }
        None => Ok((logical_id.to_owned(), None)),
    }
}

/// The partition filter terms `(field, value)` for a search/count: each injected
/// field whose value is the partition id. Isolation filters on the partition
/// field(s) only — decorative injected fields (constants, principal/header-
/// derived) are never filtered, since their value can differ between write and
/// read. Mirrors the engine's `filter_terms`.
pub(crate) fn filter_terms(transform: &BodyTransform, partition: &str) -> Vec<(FieldName, Value)> {
    let fields = match transform {
        BodyTransform::Inject(fields) | BodyTransform::Both { inject: fields, .. } => {
            fields.as_slice()
        }
        BodyTransform::None | BodyTransform::ConstructId(_) => &[],
    };
    fields
        .iter()
        .filter(|field| matches!(field.value, InjectedValue::PartitionId))
        .map(|field| (field.name.clone(), Value::String(partition.to_owned())))
        .collect()
}

/// Wrap the query body with the partition filter. With no filter terms (a
/// dedicated placement, isolated by cluster/index) the body passes through
/// unchanged; an empty body becomes `{}` before wrapping so the filter still
/// applies to a bodyless search.
pub(crate) fn filtered_query(
    body: &[u8],
    filter: &[(FieldName, Value)],
) -> Result<Vec<u8>, PrepareError> {
    if filter.is_empty() {
        return Ok(body.to_vec());
    }
    let base: &[u8] = if body.is_empty() { b"{}" } else { body };
    Ok(wrap_query(base, filter)?)
}
