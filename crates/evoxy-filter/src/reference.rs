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

use std::collections::BTreeMap;

use osproxy_core::{ClusterId, Epoch, FieldName, IndexName, PartitionId};
use osproxy_spi::{
    BodyDoc, DocIdRule, IdTemplate, InjectedField, InjectedValue, MigrationPhase, Placement,
    PlacementAt, RequestCtx, SpiError, TenancySpi,
};

/// The partition-scoped doc-id template used in shared-index mode; `{body.id}`
/// marks where the client's id goes, so it is reversible (physical↔logical).
const SHARED_ID_TEMPLATE: &str = "{partition}:{body.id}";

/// Configuration handed to the filter at Envoy module init (from the Envoy
/// `filter_config` blob). Parsed leniently: missing keys fall back to defaults so
/// a bare config still yields a runnable filter.
#[derive(Debug, Clone)]
pub struct FilterConfig {
    /// The upstream cluster id a partition routes to by default.
    pub cluster: String,
    /// Per-partition cluster overrides: a partition listed here routes to its named
    /// cluster instead of [`cluster`](Self::cluster). This is what makes the module
    /// (and ext_proc) route different tenants to different upstreams — the filter
    /// sets the resolved cluster on `x-evoxy-cluster`, and header-matched Envoy
    /// routes select it. Empty by default (single-cluster).
    pub cluster_by_partition: BTreeMap<String, String>,
    /// That cluster's base URL (carried on the placement result).
    pub endpoint: String,
    /// Per-partition endpoint overrides: a partition listed here carries its named
    /// base URL on the placement, which the filter turns into the request
    /// `:authority` for Envoy's dynamic-forward-proxy — the tenancy picks the
    /// upstream by address, with no Envoy cluster defined for it. Empty by default.
    pub endpoint_by_partition: BTreeMap<String, String>,
    /// The request header the partition id is read from.
    pub partition_header: String,
    /// When set, run shared-index mode against this physical index (isolation by
    /// injected field + partition-scoped id); otherwise dedicated-cluster mode.
    pub shared_index: Option<String>,
    /// The injected isolation field name in shared-index mode.
    pub inject_field: String,
    /// Resolve the partition from the Envoy-validated mTLS principal (the XFCC
    /// identity's `stable_id`) instead of `partition_header` (M4). Authenticated
    /// by Envoy, so a client cannot spoof it with a request header.
    pub partition_from_principal: bool,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            cluster: "opensearch".to_owned(),
            cluster_by_partition: BTreeMap::new(),
            endpoint: "http://localhost:9200".to_owned(),
            endpoint_by_partition: BTreeMap::new(),
            partition_header: "x-tenant".to_owned(),
            shared_index: None,
            inject_field: "_tenant".to_owned(),
            partition_from_principal: false,
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
        // A JSON object of partition → string (for the per-partition maps).
        let string_map = |key: &str| {
            parsed
                .get(key)
                .and_then(serde_json::Value::as_object)
                .map(|map| {
                    map.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_owned())))
                        .collect()
                })
                .unwrap_or_default()
        };
        Self {
            cluster: string("cluster", default.cluster),
            cluster_by_partition: string_map("cluster_by_partition"),
            endpoint: string("endpoint", default.endpoint),
            endpoint_by_partition: string_map("endpoint_by_partition"),
            partition_header: string("partition_header", default.partition_header),
            shared_index: parsed
                .get("shared_index")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            inject_field: string("inject_field", default.inject_field),
            partition_from_principal: parsed
                .get("partition_from_principal")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(default.partition_from_principal),
        }
    }
}

/// The reference tenancy. In dedicated mode the partition is a request header and
/// the logical index is used unchanged; in shared-index mode all partitions share
/// one physical index, isolated by an injected field and a partition-scoped doc id.
/// Routing is single-cluster by default, but `cluster_by_partition` can send named
/// partitions to different upstream clusters (header-matched routing).
#[derive(Debug, Clone)]
pub struct ReferenceTenancy {
    cluster: ClusterId,
    cluster_by_partition: BTreeMap<String, ClusterId>,
    endpoint: String,
    endpoint_by_partition: BTreeMap<String, String>,
    partition_header: String,
    shared_index: Option<IndexName>,
    inject_field: FieldName,
    partition_from_principal: bool,
    /// An in-flight migration for one partition (M5): its phase gates writes
    /// (`Cutover` holds them) and is surfaced for observability. A real fleet
    /// tenancy reads this from a `MigrationStore`; the reference carries one entry
    /// so the write gate can be exercised.
    migration: Option<(PartitionId, MigrationPhase)>,
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
            cluster_by_partition: BTreeMap::new(),
            endpoint: endpoint.into(),
            endpoint_by_partition: BTreeMap::new(),
            partition_header: header.into(),
            shared_index: None,
            inject_field: FieldName::from("_tenant"),
            partition_from_principal: false,
            migration: None,
        }
    }

    /// Mark one partition as migrating in the given phase (M5). In
    /// [`MigrationPhase::Cutover`] its writes are held (the write gate returns a
    /// retryable stale-epoch reject); reads are unaffected.
    #[must_use]
    pub fn with_migration(mut self, partition: impl Into<String>, phase: MigrationPhase) -> Self {
        self.migration = Some((PartitionId::from(partition.into().as_str()), phase));
        self
    }

    /// Construct from a parsed [`FilterConfig`] (dedicated, or shared-index when
    /// `shared_index` is set).
    #[must_use]
    pub fn from_config(config: &FilterConfig) -> Self {
        Self {
            cluster: ClusterId::from(config.cluster.as_str()),
            cluster_by_partition: config
                .cluster_by_partition
                .iter()
                .map(|(k, v)| (k.clone(), ClusterId::from(v.as_str())))
                .collect(),
            endpoint: config.endpoint.clone(),
            endpoint_by_partition: config.endpoint_by_partition.clone(),
            partition_header: config.partition_header.clone(),
            shared_index: config.shared_index.as_deref().map(IndexName::from),
            inject_field: FieldName::from(config.inject_field.as_str()),
            partition_from_principal: config.partition_from_principal,
            migration: None,
        }
    }

    /// The upstream cluster for `partition`: its per-partition override if one is
    /// configured, else the default cluster. This is the value the filter puts on
    /// `x-evoxy-cluster` for header-matched routing.
    fn cluster_for(&self, partition: &PartitionId) -> ClusterId {
        self.cluster_by_partition
            .get(partition.as_str())
            .cloned()
            .unwrap_or_else(|| self.cluster.clone())
    }

    /// The base URL for `partition`: its per-partition override if configured, else
    /// the default endpoint. Carried on the placement; the filter turns it into the
    /// `:authority` for dynamic-forward-proxy routing.
    fn endpoint_for(&self, partition: &PartitionId) -> String {
        self.endpoint_by_partition
            .get(partition.as_str())
            .cloned()
            .unwrap_or_else(|| self.endpoint.clone())
    }

    /// The migration phase for `partition` (Settled unless it is the one migrating
    /// partition).
    fn phase_of(&self, partition: &PartitionId) -> MigrationPhase {
        match &self.migration {
            Some((p, phase)) if p == partition => *phase,
            _ => MigrationPhase::Settled,
        }
    }
}

impl TenancySpi for ReferenceTenancy {
    fn resolve_partition(
        &self,
        ctx: &RequestCtx<'_>,
        _body: BodyDoc<'_>,
    ) -> Result<PartitionId, SpiError> {
        // From the Envoy-validated mTLS principal (unspoofable), or the header.
        if self.partition_from_principal {
            let principal = ctx.principal_id().as_str();
            if principal.is_empty() {
                return Err(SpiError::PartitionUnresolved { tried: Vec::new() });
            }
            return Ok(PartitionId::from(principal));
        }
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

    async fn admit_write(&self, partition: &PartitionId, _epoch: Epoch) -> bool {
        // The write gate: hold writes during the cutover window (M5, docs/06 §2).
        // A settled or draining partition admits; only cutover rejects. Epoch
        // staleness is not a factor here — the transform-then-forward model
        // resolves and forwards in one pass, so there is no resolve-to-commit gap.
        self.phase_of(partition) != MigrationPhase::Cutover
    }

    async fn placement_for(&self, partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        let cluster = self.cluster_for(partition);
        let placement = match &self.shared_index {
            Some(index) => Placement::SharedIndex {
                cluster,
                index: index.clone(),
                inject: self.injected_fields(),
            },
            None => Placement::DedicatedCluster { cluster },
        };
        Ok(PlacementAt::new(placement, Epoch::new(1))
            .with_endpoint(self.endpoint_for(partition))
            .with_phase(self.phase_of(partition)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cluster_by_partition_overrides_the_default() {
        let config = FilterConfig::from_json(
            r#"{"cluster":"os_default","cluster_by_partition":{"acme":"os_a","globex":"os_b"}}"#,
        );
        let tenancy = ReferenceTenancy::from_config(&config);

        // A mapped tenant routes to its cluster; an unmapped one to the default.
        let acme = tenancy
            .placement_for(&PartitionId::from("acme"))
            .await
            .unwrap();
        let other = tenancy
            .placement_for(&PartitionId::from("zzz"))
            .await
            .unwrap();
        assert_eq!(acme.placement.cluster().as_str(), "os_a");
        assert_eq!(other.placement.cluster().as_str(), "os_default");
    }
}
