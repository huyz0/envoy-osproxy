//! A minimal reference tenancy for the default artifact (ADR-003).
//!
//! Enough to make a runnable module with no user code. Two modes, chosen by
//! config:
//! - **dedicated** (default): every partition routes to one cluster, index name
//!   unchanged;
//! - **shared index** (`shared_index` set): all partitions share one physical
//!   index, isolated by an injected partition field and a partition-scoped doc id
//!   — enough to exercise inject/strip and id map/unmap end to end.
//!
//! A real deployment supplies its own `TenancySpi`; this is the "works out of the
//! box" default, the mirror of osproxy's `ReferenceTenancy`.

use osproxy_core::{ClusterId, Epoch, FieldName, IndexName, PartitionId};
use osproxy_spi::{
    BodyDoc, DocIdRule, IdTemplate, InjectedField, InjectedValue, Placement, PlacementAt,
    RequestCtx, SpiError, TenancySpi,
};

/// The partition-scoped doc-id template used in shared-index mode; `{body.id}`
/// marks where the client's id goes, so it is reversible (physical↔logical).
const SHARED_ID_TEMPLATE: &str = "{partition}:{body.id}";

/// Configuration handed to the filter at Envoy module init (from the Envoy
/// `filter_config` blob). Parsed leniently: missing keys fall back to defaults so
/// a bare config still yields a runnable filter.
#[derive(Debug, Clone)]
pub struct FilterConfig {
    /// The upstream cluster id every partition routes to.
    pub cluster: String,
    /// That cluster's base URL (carried on the placement result).
    pub endpoint: String,
    /// The request header the partition id is read from.
    pub partition_header: String,
    /// When set, run shared-index mode against this physical index (isolation by
    /// injected field + partition-scoped id); otherwise dedicated-cluster mode.
    pub shared_index: Option<String>,
    /// The injected isolation field name in shared-index mode.
    pub inject_field: String,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            cluster: "opensearch".to_owned(),
            endpoint: "http://localhost:9200".to_owned(),
            partition_header: "x-tenant".to_owned(),
            shared_index: None,
            inject_field: "_tenant".to_owned(),
        }
    }
}

impl FilterConfig {
    /// Parse a JSON config blob, falling back to defaults for any missing key.
    #[must_use]
    pub fn from_json(raw: &str) -> Self {
        let parsed: serde_json::Value =
            serde_json::from_str(raw).unwrap_or(serde_json::Value::Null);
        let default = Self::default();
        let string = |key: &str, fallback: String| {
            parsed
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map_or(fallback, ToOwned::to_owned)
        };
        Self {
            cluster: string("cluster", default.cluster),
            endpoint: string("endpoint", default.endpoint),
            partition_header: string("partition_header", default.partition_header),
            shared_index: parsed
                .get("shared_index")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            inject_field: string("inject_field", default.inject_field),
        }
    }
}

/// A single-cluster reference tenancy. In dedicated mode the partition is a
/// request header and the logical index is used unchanged; in shared-index mode
/// all partitions share one physical index, isolated by an injected field and a
/// partition-scoped doc id.
#[derive(Debug, Clone)]
pub struct ReferenceTenancy {
    cluster: ClusterId,
    endpoint: String,
    partition_header: String,
    shared_index: Option<IndexName>,
    inject_field: FieldName,
}

impl ReferenceTenancy {
    /// Construct a dedicated-cluster tenancy from explicit parts.
    #[must_use]
    pub fn new(
        cluster: impl Into<String>,
        endpoint: impl Into<String>,
        header: impl Into<String>,
    ) -> Self {
        Self {
            cluster: ClusterId::from(cluster.into().as_str()),
            endpoint: endpoint.into(),
            partition_header: header.into(),
            shared_index: None,
            inject_field: FieldName::from("_tenant"),
        }
    }

    /// Construct from a parsed [`FilterConfig`] (dedicated, or shared-index when
    /// `shared_index` is set).
    #[must_use]
    pub fn from_config(config: &FilterConfig) -> Self {
        Self {
            cluster: ClusterId::from(config.cluster.as_str()),
            endpoint: config.endpoint.clone(),
            partition_header: config.partition_header.clone(),
            shared_index: config.shared_index.as_deref().map(IndexName::from),
            inject_field: FieldName::from(config.inject_field.as_str()),
        }
    }
}

impl TenancySpi for ReferenceTenancy {
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
        // Shared-index isolation requires a partition-scoped id (docs/03 §4).
        self.shared_index
            .as_ref()
            .map(|_| DocIdRule::new(IdTemplate::new(SHARED_ID_TEMPLATE)).with_routing(true))
    }

    fn injected_fields(&self) -> Vec<InjectedField> {
        match &self.shared_index {
            Some(_) => vec![InjectedField::new(
                self.inject_field.clone(),
                InjectedValue::PartitionId,
            )],
            None => Vec::new(),
        }
    }

    async fn placement_for(&self, _partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        let placement = match &self.shared_index {
            Some(index) => Placement::SharedIndex {
                cluster: self.cluster.clone(),
                index: index.clone(),
                inject: self.injected_fields(),
            },
            None => Placement::DedicatedCluster {
                cluster: self.cluster.clone(),
            },
        };
        Ok(PlacementAt::new(placement, Epoch::new(1)).with_endpoint(self.endpoint.clone()))
    }
}
