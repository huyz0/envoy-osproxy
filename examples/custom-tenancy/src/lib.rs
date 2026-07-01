//! Example: a **custom `TenancySpi`** — the code a real deployment writes.
//!
//! The built-in `ReferenceTenancy` puts every tenant in the *same* physical index.
//! `TieredTenancy` instead chooses the physical index by the tenant's tier
//! (a "premium" allow-list lands in `orders_premium`, everyone else in
//! `orders_std`), while keeping the shared-index isolation model (an injected
//! `_tenant` field + a partition-scoped, reversible doc id). Choosing placement
//! per tenant is exactly what you implement the SPI for.
//!
//! It is built from the same `osproxy-spi` traits the standalone proxy uses;
//! nothing here knows about Envoy. You hand this to `evoxy-filter`, build a
//! dynamic-module `.so`, and Envoy drives it (see `examples/README.md`). This uses
//! only mechanisms the module honors end-to-end today — physical-index path
//! rewrite, field injection, id map/unmap, and response reshaping.
#![deny(missing_docs)]

use std::collections::BTreeSet;

use osproxy_core::{ClusterId, Epoch, FieldName, IndexName, PartitionId};
use osproxy_spi::{
    BodyDoc, DocIdRule, IdTemplate, InjectedField, InjectedValue, Placement, PlacementAt,
    RequestCtx, SpiError, TenancySpi,
};

/// Reversible partition-scoped id: `{body.id}` marks where the client's id goes,
/// so physical (`acme:42`) maps back to logical (`42`) on the way out.
const ID_TEMPLATE: &str = "{partition}:{body.id}";

/// Places each tenant into a per-tier shared index on one cluster, isolated by an
/// injected field and a partition-scoped id.
#[derive(Debug, Clone)]
pub struct TieredTenancy {
    /// The request header carrying the tenant id.
    pub partition_header: String,
    /// The upstream cluster (mapped to a real OpenSearch in the Envoy bootstrap).
    pub cluster: String,
    /// Tenants in this set are placed in the premium index; all others in standard.
    pub premium: BTreeSet<String>,
}

impl TieredTenancy {
    /// The physical index for a partition, by tier.
    fn index_for(&self, partition: &PartitionId) -> IndexName {
        if self.premium.contains(partition.as_str()) {
            IndexName::from("orders_premium")
        } else {
            IndexName::from("orders_std")
        }
    }
}

impl TenancySpi for TieredTenancy {
    fn resolve_partition(
        &self,
        ctx: &RequestCtx<'_>,
        _body: BodyDoc<'_>,
    ) -> Result<PartitionId, SpiError> {
        ctx.headers()
            .get(&self.partition_header)
            .map(PartitionId::from)
            .ok_or(SpiError::PartitionUnresolved { tried: Vec::new() })
    }

    fn doc_id_rule(&self) -> Option<DocIdRule> {
        Some(DocIdRule::new(IdTemplate::new(ID_TEMPLATE)).with_routing(true))
    }

    fn injected_fields(&self) -> Vec<InjectedField> {
        vec![InjectedField::new(
            FieldName::from("_tenant"),
            InjectedValue::PartitionId,
        )]
    }

    async fn placement_for(&self, partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        Ok(PlacementAt::new(
            Placement::SharedIndex {
                cluster: ClusterId::from(self.cluster.as_str()),
                index: self.index_for(partition),
                inject: self.injected_fields(),
            },
            Epoch::new(1),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenancy() -> TieredTenancy {
        TieredTenancy {
            partition_header: "x-tenant".to_owned(),
            cluster: "opensearch".to_owned(),
            premium: ["acme".to_owned()].into_iter().collect(),
        }
    }

    #[test]
    fn premium_and_standard_tenants_get_different_indices() {
        let t = tenancy();
        assert_eq!(
            t.index_for(&PartitionId::from("acme")).as_str(),
            "orders_premium"
        );
        assert_eq!(
            t.index_for(&PartitionId::from("globex")).as_str(),
            "orders_std"
        );
    }
}
