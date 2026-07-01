//! Applying a `BodyTransform` to the request body — the transform-then-forward
//! analog of the engine's `plan.rs`.
//!
//! osproxy's `plan.rs` applies the same transform but targets the `Sink`
//! (dispatch): it produces a `WriteBatch`/`DocOp`. We do **not** dispatch
//! (ADR-002), so this module applies the transform over the shared
//! [`osproxy_rewrite`] byte-splice primitives and yields the pieces Envoy needs
//! to forward — the constructed id, the mutated body, and the `_routing` value.
//! The primitives are reused; only this thin forward-shaped glue is ours.

use osproxy_core::FieldName;
use osproxy_rewrite::{construct_id_bytes, inject_fields_bytes, validate_json};
use osproxy_spi::{BodyTransform, DocIdRule, InjectedField, InjectedValue};
use serde_json::Value;

use crate::PrepareError;

/// The result of applying a body transform: the id constructed by the transform
/// (if any), the bytes to forward, and the `_routing` value (if the id rule set
/// it).
pub(crate) struct Transformed {
    pub(crate) id: Option<String>,
    pub(crate) body: Vec<u8>,
    pub(crate) routing: Option<String>,
}

/// Apply `transform` to `body` for `partition`. Mirrors the engine's
/// `apply_transform`: an untouched body is still validated as a JSON object; an
/// injected body is spliced; an id is read straight from the original bytes.
pub(crate) fn apply(
    body: &[u8],
    transform: &BodyTransform,
    partition: &str,
) -> Result<Transformed, PrepareError> {
    let (id, out) = match transform {
        BodyTransform::None => {
            validate_json(body)?;
            (None, body.to_vec())
        }
        BodyTransform::Inject(fields) => (None, inject(body, fields, partition)?),
        BodyTransform::ConstructId(rule) => {
            validate_json(body)?;
            (Some(build_id(rule, body, partition)?), body.to_vec())
        }
        BodyTransform::Both { inject: fields, id } => {
            let out = inject(body, fields, partition)?;
            (Some(build_id(id, body, partition)?), out)
        }
    };
    Ok(Transformed {
        id,
        body: out,
        routing: routing_for(transform, partition),
    })
}

/// Splice the resolved fields into the body bytes.
fn inject(body: &[u8], fields: &[InjectedField], partition: &str) -> Result<Vec<u8>, PrepareError> {
    let resolved = resolve_values(fields, partition)?;
    Ok(inject_fields_bytes(body, &resolved)?)
}

/// Construct the `_id` from a rule by reading scalars straight from the bytes.
fn build_id(rule: &DocIdRule, body: &[u8], partition: &str) -> Result<String, PrepareError> {
    Ok(construct_id_bytes(rule.template.as_str(), partition, body)?)
}

/// The `_routing` value: the partition id, but only when a constructing
/// transform asked for it (`set_routing`).
fn routing_for(transform: &BodyTransform, partition: &str) -> Option<String> {
    let rule = match transform {
        BodyTransform::ConstructId(rule) | BodyTransform::Both { id: rule, .. } => Some(rule),
        BodyTransform::None | BodyTransform::Inject(_) => None,
    };
    rule.filter(|r| r.set_routing).map(|_| partition.to_owned())
}

/// Resolve each injected field to a concrete JSON value. The tenancy router has
/// already resolved context-derived values to [`InjectedValue::Constant`]
/// (`resolve_inject`); [`InjectedValue::PartitionId`] resolves to the partition
/// string here. A `FromPrincipal`/`FromHeader` reaching this point is an
/// invariant violation (the router should have resolved it), so it is an
/// internal error, not a client error.
fn resolve_values(
    fields: &[InjectedField],
    partition: &str,
) -> Result<Vec<(FieldName, Value)>, PrepareError> {
    fields
        .iter()
        .map(|field| {
            let value = match &field.value {
                InjectedValue::Constant(v) => v.clone(),
                InjectedValue::PartitionId => Value::String(partition.to_owned()),
                InjectedValue::FromPrincipal(_) | InjectedValue::FromHeader(_) => {
                    return Err(PrepareError::UnresolvedInjectedValue)
                }
            };
            Ok((field.name.clone(), value))
        })
        .collect()
}
