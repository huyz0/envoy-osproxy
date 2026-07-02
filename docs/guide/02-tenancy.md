# Implementing a tenancy

Your placement and isolation logic is a Rust type that implements one trait,
`osproxy_spi::TenancySpi`. This is the same trait the standalone osproxy uses, so
the logic is identical whether it runs in osproxy's server or inside Envoy. You do
not write a `main`, an upstream client, or any TLS. Envoy is the app and forwards
upstream for you.

If you just want to try envoy-osproxy without writing code, skip this page. The
built-in reference tenancy partitions by a request header or the mTLS principal and
supports dedicated-cluster or shared-index isolation, all from Envoy's filter
config. Come back here when you need placement logic the reference tenancy does not
cover.

## The four methods you implement

A tenancy answers four questions per request:

- `resolve_partition`: which tenant does this request belong to?
- `doc_id_rule`: does a written document get a partition-scoped id?
- `injected_fields`: which fields are added to isolate a document?
- `placement_for`: which cluster and index does the tenant live on?

Three more methods have defaults you rarely override: `admit_write` (used by the
migration write gate), `sensitive_fields`, and `cluster_endpoint`.

## A worked example

This tenancy places each tenant into one of two shared indices by tier. A tenant on
the premium allow-list lands in `orders_premium`; everyone else lands in
`orders_std`. Both use shared-index isolation: an injected `_tenant` field and a
partition-scoped document id, so one physical index safely holds many tenants and
each still reads only its own documents.

The full crate is
[`examples/custom-tenancy`](https://github.com/huyz0/envoy-osproxy/tree/main/examples/custom-tenancy),
and it compiles and is unit-tested as part of the workspace.

```rust
use std::collections::BTreeSet;

use osproxy_core::{ClusterId, Epoch, FieldName, IndexName, PartitionId};
use osproxy_spi::{
    BodyDoc, DocIdRule, IdTemplate, InjectedField, InjectedValue, Placement, PlacementAt,
    RequestCtx, SpiError, TenancySpi,
};

// A reversible partition-scoped id. `{body.id}` marks where the client's id goes,
// so the physical id `acme:42` maps back to the logical id `42` on the way out.
const ID_TEMPLATE: &str = "{partition}:{body.id}";

pub struct TieredTenancy {
    pub partition_header: String,
    pub cluster: String,
    pub premium: BTreeSet<String>,
}

impl TieredTenancy {
    fn index_for(&self, partition: &PartitionId) -> IndexName {
        if self.premium.contains(partition.as_str()) {
            IndexName::from("orders_premium")
        } else {
            IndexName::from("orders_std")
        }
    }
}

impl TenancySpi for TieredTenancy {
    // Read the tenant id from a request header. Return a fail-closed error if it is
    // absent, so an unattributed request is never routed.
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

    // Give each written document a partition-scoped id, so two tenants can use the
    // same natural key without colliding in a shared index.
    fn doc_id_rule(&self) -> Option<DocIdRule> {
        Some(DocIdRule::new(IdTemplate::new(ID_TEMPLATE)).with_routing(true))
    }

    // Inject a `_tenant` field carrying the partition id, so a search can filter to
    // one tenant and a read can confirm ownership.
    fn injected_fields(&self) -> Vec<InjectedField> {
        vec![InjectedField::new(FieldName::from("_tenant"), InjectedValue::PartitionId)]
    }

    // Place the partition on one cluster, in the tier's physical index, with the
    // injected field. The `ClusterId` is mapped to a real upstream in Envoy config.
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
```

## What the engine does with it

Nothing here talks to Envoy or OpenSearch. The engine reads these answers and drives
the transform:

- On a write, it rewrites the request path to the physical index from
  `placement_for`, builds the document id from `doc_id_rule`, and splices the
  injected fields from `injected_fields` into the body.
- On a read, it pins the query to the physical index, adds a filter on the injected
  field so a search sees only the tenant's documents, then reshapes the response
  back to the client's logical view by stripping the injected field and mapping the
  physical id to the logical one.

Because the id template is reversible, the client sends and receives its own
`orders/_doc/42` while the physical document lives at `orders_premium/_doc/acme:42`.

## Placement kinds

`placement_for` returns one of a few placement shapes:

- `Placement::DedicatedCluster { cluster }`: the whole tenant lives on one cluster,
  index name unchanged. No isolation transform.
- `Placement::DedicatedIndex { cluster, index }`: a per-tenant physical index on one
  cluster.
- `Placement::SharedIndex { cluster, index, inject }`: many tenants share one
  physical index, isolated by the injected field and a partition-scoped id. This is
  the example above.

Returning a different `cluster` per request routes to a different upstream on both
backends: the filter sets the resolved cluster on the `x-evoxy-cluster` header and
Envoy selects the upstream from header-matched routes. Your bootstrap needs those
routes — see [Building the dynamic module](04-build-module.md).

## Next

Wire your tenancy into a backend: [ext_proc](03-build-extproc.md) or the
[dynamic module](04-build-module.md).
