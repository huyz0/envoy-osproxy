//! A minimal reference tenancy for the default artifact (ADR-003).
//!
//! Enough to make a runnable module with no user code: it keys the partition off
//! a configurable header and routes every partition to one dedicated cluster
//! (index name unchanged). A real deployment supplies its own `TenancySpi`; this
//! is the "works out of the box" default, the mirror of osproxy's
//! `ReferenceTenancy`.

use osproxy_core::{ClusterId, Epoch, PartitionId};
use osproxy_spi::{
    BodyDoc, DocIdRule, InjectedField, Placement, PlacementAt, RequestCtx, SpiError, TenancySpi,
};

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
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            cluster: "opensearch".to_owned(),
            endpoint: "http://localhost:9200".to_owned(),
            partition_header: "x-tenant".to_owned(),
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
        }
    }
}

/// A single-cluster passthrough tenancy: the partition is a request header; every
/// partition is placed on one dedicated cluster with the logical index unchanged.
#[derive(Debug, Clone)]
pub struct ReferenceTenancy {
    cluster: ClusterId,
    endpoint: String,
    partition_header: String,
}

impl ReferenceTenancy {
    /// Construct from explicit parts.
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
        }
    }

    /// Construct from a parsed [`FilterConfig`].
    #[must_use]
    pub fn from_config(config: &FilterConfig) -> Self {
        Self::new(&config.cluster, &config.endpoint, &config.partition_header)
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
        None
    }

    fn injected_fields(&self) -> Vec<InjectedField> {
        Vec::new()
    }

    async fn placement_for(&self, _partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        Ok(PlacementAt::new(
            Placement::DedicatedCluster {
                cluster: self.cluster.clone(),
            },
            Epoch::new(1),
        )
        .with_endpoint(self.endpoint.clone()))
    }
}
