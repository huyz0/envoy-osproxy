//! Config-driven placement tests for the reference tenancy: one per isolation mode
//! plus partition-source and default-partition behavior.

use osproxy_core::{EndpointKind, PrincipalId, RequestId};
use osproxy_spi::{BodyDoc, HeaderView, HttpMethod, Principal, Protocol};

use super::*;

async fn placement(config: &str, partition: &str) -> Placement {
    let tenancy = ReferenceTenancy::from_config(&FilterConfig::from_json(config));
    tenancy
        .placement_for(&PartitionId::from(partition))
        .await
        .unwrap()
        .placement
}

/// The physical index of an index-bearing placement (`None` for `DedicatedCluster`).
/// `expect` is allowed in tests; `panic!` is not, so the wrong variant funnels
/// through an `Option`.
fn physical_index(placement: &Placement) -> Option<String> {
    match placement {
        Placement::DedicatedIndex { index, .. } | Placement::SharedIndex { index, .. } => {
            Some(index.as_str().to_owned())
        }
        Placement::DedicatedCluster { .. } => None,
    }
}

#[tokio::test]
async fn dedicated_cluster_is_the_default_and_keeps_the_index() {
    // No isolation key, no shared_index: dedicated cluster (the engine keeps the
    // client's index for DedicatedCluster, so the placement carries no index).
    let pl = placement(r#"{"cluster":"os"}"#, "acme").await;
    assert!(matches!(pl, Placement::DedicatedCluster { .. }));
    assert_eq!(pl.cluster().as_str(), "os");
}

#[tokio::test]
async fn cluster_by_partition_overrides_the_default() {
    let cfg = r#"{"cluster":"os_default","cluster_by_partition":{"acme":"os_a"}}"#;
    assert_eq!(placement(cfg, "acme").await.cluster().as_str(), "os_a");
    assert_eq!(placement(cfg, "zzz").await.cluster().as_str(), "os_default");
}

#[tokio::test]
async fn dedicated_index_templates_a_per_tenant_index() {
    let cfg =
        r#"{"isolation":"dedicated_index","cluster":"os","index_template":"orders-{partition}"}"#;
    let pl = placement(cfg, "acme").await;
    assert!(matches!(pl, Placement::DedicatedIndex { .. }));
    assert_eq!(pl.cluster().as_str(), "os");
    assert_eq!(physical_index(&pl).expect("an index"), "orders-acme");
    // The default template is the bare partition.
    let bare = placement(r#"{"isolation":"dedicated_index"}"#, "globex").await;
    assert_eq!(physical_index(&bare).expect("an index"), "globex");
}

#[tokio::test]
async fn shared_index_is_inferred_and_injects() {
    // `shared_index` present but no `isolation` key: inferred as shared.
    let tenancy = ReferenceTenancy::from_config(&FilterConfig::from_json(
        r#"{"shared_index":"orders_shared","inject_field":"_t"}"#,
    ));
    let pl = tenancy
        .placement_for(&PartitionId::from("acme"))
        .await
        .unwrap()
        .placement;
    assert!(matches!(pl, Placement::SharedIndex { .. }));
    assert_eq!(physical_index(&pl).expect("an index"), "orders_shared");
    assert!(
        tenancy.doc_id_rule().is_some(),
        "shared mode constructs an id"
    );
    assert_eq!(tenancy.injected_fields().len(), 1);
}

#[test]
fn non_shared_modes_do_not_inject_or_construct_ids() {
    let dedicated = ReferenceTenancy::from_config(&FilterConfig::from_json(r#"{"cluster":"os"}"#));
    assert!(dedicated.doc_id_rule().is_none());
    assert!(dedicated.injected_fields().is_empty());
}

/// Build a minimal `RequestCtx` with the given headers, for `resolve_partition`.
fn ctx<'a>(
    principal: &'a Principal,
    id: &'a RequestId,
    headers: &'a [(String, String)],
) -> RequestCtx<'a> {
    RequestCtx::new(
        principal,
        id,
        HttpMethod::Post,
        EndpointKind::IngestDoc,
        Protocol::Http1,
        "orders",
        HeaderView::new(headers),
        b"{}",
    )
}

#[test]
fn default_partition_is_used_when_the_source_is_missing() {
    let tenancy = ReferenceTenancy::from_config(&FilterConfig::from_json(
        r#"{"default_partition":"public"}"#,
    ));
    let principal = Principal::new(PrincipalId::from("anonymous"));
    let id = RequestId::from("r");
    let headers: Vec<(String, String)> = Vec::new();
    let resolved = tenancy
        .resolve_partition(&ctx(&principal, &id, &headers), BodyDoc::new(b"{}"))
        .unwrap();
    assert_eq!(resolved.as_str(), "public");
}

#[test]
fn no_default_fails_closed_when_unresolved() {
    let tenancy = ReferenceTenancy::from_config(&FilterConfig::from_json(r#"{"cluster":"os"}"#));
    let principal = Principal::new(PrincipalId::from("anonymous"));
    let id = RequestId::from("r");
    let headers: Vec<(String, String)> = Vec::new();
    assert!(tenancy
        .resolve_partition(&ctx(&principal, &id, &headers), BodyDoc::new(b"{}"))
        .is_err());
}

// ---- Deep-review coverage: migration gate, partition-source edges, parsing ----

/// The migration write gate: a partition in cutover has writes held; every other
/// partition (and every other phase) admits, and the phase rides the placement.
#[tokio::test]
async fn cutover_holds_writes_and_carries_the_phase() {
    let frozen = ReferenceTenancy::new("os", "http://os:9200", "x-tenant")
        .with_migration("frozen", MigrationPhase::Cutover);

    // The migrating partition is held; an unrelated one admits.
    assert!(
        !frozen
            .admit_write(&PartitionId::from("frozen"), Epoch::new(1))
            .await
    );
    assert!(
        frozen
            .admit_write(&PartitionId::from("acme"), Epoch::new(1))
            .await
    );

    // The phase is carried on the placement for the migrating partition only.
    let at = frozen
        .placement_for(&PartitionId::from("frozen"))
        .await
        .unwrap();
    assert_eq!(at.phase, MigrationPhase::Cutover);
    let at = frozen
        .placement_for(&PartitionId::from("acme"))
        .await
        .unwrap();
    assert_eq!(at.phase, MigrationPhase::Settled);

    // A draining partition still admits (only cutover rejects).
    let draining = ReferenceTenancy::new("os", "http://os:9200", "x-tenant")
        .with_migration("d", MigrationPhase::Draining);
    assert!(
        draining
            .admit_write(&PartitionId::from("d"), Epoch::new(1))
            .await
    );
}

/// An explicit `partition_source: header` (any non-principal value) selects the
/// header source even when the legacy bool would say otherwise.
#[test]
fn explicit_header_source_overrides_the_legacy_bool() {
    let cfg =
        FilterConfig::from_json(r#"{"partition_source":"header","partition_from_principal":true}"#);
    assert!(!cfg.partition_from_principal);
    assert!(!cfg.partition_from_path);
}

/// An explicit `dedicated_cluster` wins even when `shared_index` is present.
#[test]
fn explicit_dedicated_cluster_overrides_the_inference() {
    let cfg = FilterConfig::from_json(r#"{"isolation":"dedicated_cluster","shared_index":"s"}"#);
    assert!(matches!(cfg.isolation, Isolation::DedicatedCluster));
}

/// Non-string entries in `passthrough_indices` are skipped, not an error.
#[test]
fn passthrough_array_skips_non_strings() {
    let cfg = FilterConfig::from_json(r#"{"passthrough_indices":["catalog",7,null,"logs"]}"#);
    assert_eq!(cfg.passthrough_indices, vec!["catalog", "logs"]);
}

/// Principal source with no presented principal falls back to `default_partition`.
#[test]
fn empty_principal_falls_back_to_the_default_partition() {
    let tenancy = ReferenceTenancy::from_config(&FilterConfig::from_json(
        r#"{"partition_source":"principal","default_partition":"shared"}"#,
    ));
    // An anonymous request carries an empty principal id.
    let principal = Principal::new(PrincipalId::from(""));
    let id = RequestId::from("r");
    let headers: Vec<(String, String)> = Vec::new();
    let partition = tenancy
        .resolve_partition(&ctx(&principal, &id, &headers), BodyDoc::new(b"{}"))
        .unwrap();
    assert_eq!(partition.as_str(), "shared");
}
