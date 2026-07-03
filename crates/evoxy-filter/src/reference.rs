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
    /// Take the tenant from the first path segment (`partition_source: path`): the
    /// filter strips it and moves it into `partition_header` before routing.
    pub partition_from_path: bool,
    /// Logical indices that bypass tenancy entirely (forwarded unchanged), for
    /// global or shared indices that need no isolation. Empty by default.
    pub passthrough_indices: Vec<String>,
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
            partition_from_path: false,
            passthrough_indices: Vec::new(),
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
        let v: serde_json::Value = serde_json::from_str(raw).unwrap_or(serde_json::Value::Null);
        let d = Self::default();
        let shared_index = opt_str(&v, "shared_index");
        // `partition_source`: header (default), principal, or path. The legacy
        // `partition_from_principal` bool still selects the principal source.
        let source = v
            .get("partition_source")
            .and_then(serde_json::Value::as_str)
            .map(str::to_ascii_lowercase);
        let from_principal = match source.as_deref() {
            Some("principal") => true,
            Some(_) => false,
            None => bool_at(&v, "partition_from_principal", d.partition_from_principal),
        };
        Self {
            isolation: parse_isolation(v.get("isolation"), shared_index.is_some()),
            cluster: str_at(&v, "cluster", d.cluster),
            cluster_by_partition: str_map(&v, "cluster_by_partition"),
            endpoint: str_at(&v, "endpoint", d.endpoint),
            endpoint_by_partition: str_map(&v, "endpoint_by_partition"),
            partition_header: str_at(&v, "partition_header", d.partition_header),
            partition_from_principal: from_principal,
            partition_from_path: source.as_deref() == Some("path"),
            passthrough_indices: str_array(&v, "passthrough_indices"),
            default_partition: opt_str(&v, "default_partition"),
            index_template: str_at(&v, "index_template", d.index_template),
            shared_index,
            inject_field: str_at(&v, "inject_field", d.inject_field),
            id_template: str_at(&v, "id_template", d.id_template),
            routing: bool_at(&v, "routing", d.routing),
        }
    }
}

/// A string key, or `fallback` when absent.
fn str_at(v: &serde_json::Value, key: &str, fallback: String) -> String {
    v.get(key)
        .and_then(serde_json::Value::as_str)
        .map_or(fallback, ToOwned::to_owned)
}

/// An optional string key.
fn opt_str(v: &serde_json::Value, key: &str) -> Option<String> {
    v.get(key)
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
}

/// A bool key, or `fallback` when absent.
fn bool_at(v: &serde_json::Value, key: &str, fallback: bool) -> bool {
    v.get(key)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(fallback)
}

/// A JSON object of string to string (the per-partition maps).
fn str_map(v: &serde_json::Value, key: &str) -> BTreeMap<String, String> {
    v.get(key)
        .and_then(serde_json::Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_owned())))
                .collect()
        })
        .unwrap_or_default()
}

/// A JSON array of strings (logical index names, for passthrough).
fn str_array(v: &serde_json::Value, key: &str) -> Vec<String> {
    v.get(key)
        .and_then(serde_json::Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|val| val.as_str().map(ToOwned::to_owned))
                .collect()
        })
        .unwrap_or_default()
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
