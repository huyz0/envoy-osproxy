//! The config-driven reference tenancy for the default artifact (ADR-003).
//!
//! This is the "works out of the box" tenancy: a whole multi-tenancy driven by the
//! Envoy `filter_config` blob, no `TenancySpi` to implement. It covers the common
//! OpenSearch patterns, chosen by `isolation`:
//! - `dedicated_cluster` (default): the client's index is kept; requests route to
//!   one cluster, or a per-tenant cluster/endpoint.
//! - `dedicated_index`: one cluster, a per-tenant physical index from
//!   `index_template` (physical isolation, no injected field).
//! - `shared_index`: all tenants share one physical index, isolated by an injected
//!   field and a partition-scoped id.
//!
//! A deployment whose needs go beyond this supplies its own `TenancySpi`.

use std::collections::BTreeMap;

use osproxy_core::{ClusterId, Epoch, FieldName, IndexName, PartitionId};
use osproxy_spi::{
    BodyDoc, DocIdRule, IdTemplate, InjectedField, InjectedValue, MigrationPhase, Placement,
    PlacementAt, RequestCtx, SpiError, TenancySpi,
};

/// The default partition-scoped doc-id template for shared-index mode; `{body.id}`
/// marks where the client's id goes, so it is reversible (physical to logical).
const DEFAULT_ID_TEMPLATE: &str = "{partition}:{body.id}";
/// The default per-tenant physical index for dedicated-index mode.
const DEFAULT_INDEX_TEMPLATE: &str = "{partition}";

/// How the reference tenancy isolates tenants. Selected by the `isolation` config
/// key; when absent it is inferred (`shared_index` set implies [`SharedIndex`], else
/// [`DedicatedCluster`]).
///
/// [`SharedIndex`]: Isolation::SharedIndex
/// [`DedicatedCluster`]: Isolation::DedicatedCluster
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Isolation {
    /// Keep the client's index; isolate by cluster (optionally per tenant).
    DedicatedCluster,
    /// One cluster, a per-tenant physical index from `index_template`.
    DedicatedIndex,
    /// One physical index shared by all tenants, isolated by an injected field.
    SharedIndex,
}

/// Configuration handed to the filter at init (from the Envoy `filter_config`
/// blob). Parsed leniently: missing keys fall back to defaults, so a bare config
/// still yields a runnable filter.
#[derive(Debug, Clone)]
pub struct FilterConfig {
    /// How to isolate tenants (see [`Isolation`]).
    pub isolation: Isolation,
    /// The upstream cluster id a partition routes to by default.
    pub cluster: String,
    /// Per-partition cluster overrides: a partition here routes to its named cluster
    /// instead of [`cluster`](Self::cluster). This is what routes different tenants
    /// to different upstreams (the filter sets `x-evoxy-cluster` and header-matched
    /// Envoy routes select it). Empty by default (single cluster).
    pub cluster_by_partition: BTreeMap<String, String>,
    /// That cluster's base URL (carried on the placement result).
    pub endpoint: String,
    /// Per-partition endpoint overrides: a partition here carries its named base URL
    /// on the placement, which the filter turns into the request `:authority` for
    /// Envoy's dynamic-forward-proxy (routing by address, no Envoy cluster defined).
    pub endpoint_by_partition: BTreeMap<String, String>,
    /// The request header the partition id is read from (`partition_source: header`).
    pub partition_header: String,
    /// Resolve the partition from the Envoy-validated mTLS principal instead of the
    /// header (`partition_source: principal`). Unspoofable, since Envoy validated it.
    pub partition_from_principal: bool,
    /// Used when the partition source is missing, instead of failing closed. Enables
    /// single-tenant deployments and graceful defaults. `None` fails closed.
    pub default_partition: Option<String>,
    /// The per-tenant physical index template for `dedicated_index` (`{partition}`
    /// is substituted), e.g. `"{partition}"` or `"orders-{partition}"`.
    pub index_template: String,
    /// The physical index shared by all tenants in `shared_index` mode.
    pub shared_index: Option<String>,
    /// The injected isolation field name in `shared_index` mode.
    pub inject_field: String,
    /// The partition-scoped doc-id template in `shared_index` mode.
    pub id_template: String,
    /// Whether `shared_index` sets `?routing=` (co-locate a partition's docs).
    pub routing: bool,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            isolation: Isolation::DedicatedCluster,
            cluster: "opensearch".to_owned(),
            cluster_by_partition: BTreeMap::new(),
            endpoint: "http://localhost:9200".to_owned(),
            endpoint_by_partition: BTreeMap::new(),
            partition_header: "x-tenant".to_owned(),
            partition_from_principal: false,
            default_partition: None,
            index_template: DEFAULT_INDEX_TEMPLATE.to_owned(),
            shared_index: None,
            inject_field: "_tenant".to_owned(),
            id_template: DEFAULT_ID_TEMPLATE.to_owned(),
            routing: true,
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
        let opt_string = |key: &str| {
            parsed
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned)
        };
        let bool_of = |key: &str, fallback: bool| {
            parsed
                .get(key)
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(fallback)
        };
        // A JSON object of partition to string (for the per-partition maps).
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
        let shared_index = opt_string("shared_index");
        // `partition_source: principal` and the legacy `partition_from_principal`.
        let from_principal = parsed
            .get("partition_source")
            .and_then(serde_json::Value::as_str)
            .map_or_else(
                || bool_of("partition_from_principal", default.partition_from_principal),
                |s| s.eq_ignore_ascii_case("principal"),
            );
        Self {
            isolation: parse_isolation(parsed.get("isolation"), shared_index.is_some()),
            cluster: string("cluster", default.cluster),
            cluster_by_partition: string_map("cluster_by_partition"),
            endpoint: string("endpoint", default.endpoint),
            endpoint_by_partition: string_map("endpoint_by_partition"),
            partition_header: string("partition_header", default.partition_header),
            partition_from_principal: from_principal,
            default_partition: opt_string("default_partition"),
            index_template: string("index_template", default.index_template),
            shared_index,
            inject_field: string("inject_field", default.inject_field),
            id_template: string("id_template", default.id_template),
            routing: bool_of("routing", default.routing),
        }
    }
}

/// Map the `isolation` config value to an [`Isolation`], inferring it from
/// `shared_index` when the key is absent or unrecognized.
fn parse_isolation(value: Option<&serde_json::Value>, has_shared_index: bool) -> Isolation {
    match value.and_then(serde_json::Value::as_str) {
        Some(s) if s.eq_ignore_ascii_case("shared_index") => Isolation::SharedIndex,
        Some(s) if s.eq_ignore_ascii_case("dedicated_index") => Isolation::DedicatedIndex,
        Some(s) if s.eq_ignore_ascii_case("dedicated_cluster") => Isolation::DedicatedCluster,
        _ if has_shared_index => Isolation::SharedIndex,
        _ => Isolation::DedicatedCluster,
    }
}

/// The reference tenancy: a config-driven multi-tenancy covering the common
/// isolation models (see the module docs and [`Isolation`]).
#[derive(Debug, Clone)]
pub struct ReferenceTenancy {
    isolation: Isolation,
    cluster: ClusterId,
    cluster_by_partition: BTreeMap<String, ClusterId>,
    endpoint: String,
    endpoint_by_partition: BTreeMap<String, String>,
    partition_header: String,
    partition_from_principal: bool,
    default_partition: Option<PartitionId>,
    index_template: String,
    shared_index: IndexName,
    inject_field: FieldName,
    id_template: String,
    routing: bool,
    /// An in-flight migration for one partition (M5): its phase gates writes
    /// (`Cutover` holds them). A real fleet tenancy reads this from a
    /// `MigrationStore`; the reference carries one entry so the gate is exercisable.
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
        Self::from_config(&FilterConfig {
            cluster: cluster.into(),
            endpoint: endpoint.into(),
            partition_header: header.into(),
            ..FilterConfig::default()
        })
    }

    /// Mark one partition as migrating in the given phase (M5). In
    /// [`MigrationPhase::Cutover`] its writes are held (a retryable stale-epoch
    /// reject); reads are unaffected.
    #[must_use]
    pub fn with_migration(mut self, partition: impl Into<String>, phase: MigrationPhase) -> Self {
        self.migration = Some((PartitionId::from(partition.into().as_str()), phase));
        self
    }

    /// Construct from a parsed [`FilterConfig`].
    #[must_use]
    pub fn from_config(config: &FilterConfig) -> Self {
        Self {
            isolation: config.isolation,
            cluster: ClusterId::from(config.cluster.as_str()),
            cluster_by_partition: config
                .cluster_by_partition
                .iter()
                .map(|(k, v)| (k.clone(), ClusterId::from(v.as_str())))
                .collect(),
            endpoint: config.endpoint.clone(),
            endpoint_by_partition: config.endpoint_by_partition.clone(),
            partition_header: config.partition_header.clone(),
            partition_from_principal: config.partition_from_principal,
            default_partition: config.default_partition.as_deref().map(PartitionId::from),
            index_template: config.index_template.clone(),
            // Shared mode needs a physical index name; default it when omitted.
            shared_index: IndexName::from(config.shared_index.as_deref().unwrap_or("shared")),
            inject_field: FieldName::from(config.inject_field.as_str()),
            id_template: config.id_template.clone(),
            routing: config.routing,
            migration: None,
        }
    }

    /// The upstream cluster for `partition`: its per-partition override if one is
    /// configured, else the default cluster.
    fn cluster_for(&self, partition: &PartitionId) -> ClusterId {
        self.cluster_by_partition
            .get(partition.as_str())
            .cloned()
            .unwrap_or_else(|| self.cluster.clone())
    }

    /// The base URL for `partition`: its per-partition override, else the default.
    fn endpoint_for(&self, partition: &PartitionId) -> String {
        self.endpoint_by_partition
            .get(partition.as_str())
            .cloned()
            .unwrap_or_else(|| self.endpoint.clone())
    }

    /// The per-tenant physical index for `dedicated_index` mode.
    fn index_for(&self, partition: &PartitionId) -> IndexName {
        IndexName::from(
            self.index_template
                .replace("{partition}", partition.as_str())
                .as_str(),
        )
    }

    /// The migration phase for `partition` (Settled unless it is the migrating one).
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
        let raw = if self.partition_from_principal {
            let principal = ctx.principal_id().as_str();
            (!principal.is_empty()).then(|| PartitionId::from(principal))
        } else {
            ctx.headers()
                .get(&self.partition_header)
                .map(PartitionId::from)
        };
        raw.or_else(|| self.default_partition.clone())
            .ok_or(SpiError::PartitionUnresolved { tried: Vec::new() })
    }

    fn doc_id_rule(&self) -> Option<DocIdRule> {
        // Only shared-index isolation needs a partition-scoped id (docs/03 §4).
        match self.isolation {
            Isolation::SharedIndex => {
                Some(DocIdRule::new(IdTemplate::new(&self.id_template)).with_routing(self.routing))
            }
            _ => None,
        }
    }

    fn injected_fields(&self) -> Vec<InjectedField> {
        match self.isolation {
            Isolation::SharedIndex => vec![InjectedField::new(
                self.inject_field.clone(),
                InjectedValue::PartitionId,
            )],
            _ => Vec::new(),
        }
    }

    async fn admit_write(&self, partition: &PartitionId, _epoch: Epoch) -> bool {
        // The write gate: hold writes during the cutover window (M5, docs/06 §2). A
        // settled or draining partition admits; only cutover rejects. Epoch staleness
        // is not a factor: transform-then-forward resolves and forwards in one pass.
        self.phase_of(partition) != MigrationPhase::Cutover
    }

    async fn placement_for(&self, partition: &PartitionId) -> Result<PlacementAt, SpiError> {
        let cluster = self.cluster_for(partition);
        let placement = match self.isolation {
            Isolation::DedicatedCluster => Placement::DedicatedCluster { cluster },
            Isolation::DedicatedIndex => Placement::DedicatedIndex {
                cluster,
                index: self.index_for(partition),
            },
            Isolation::SharedIndex => Placement::SharedIndex {
                cluster,
                index: self.shared_index.clone(),
                inject: self.injected_fields(),
            },
        };
        Ok(PlacementAt::new(placement, Epoch::new(1))
            .with_endpoint(self.endpoint_for(partition))
            .with_phase(self.phase_of(partition)))
    }
}

#[cfg(test)]
#[path = "reference_tests.rs"]
mod reference_tests;
