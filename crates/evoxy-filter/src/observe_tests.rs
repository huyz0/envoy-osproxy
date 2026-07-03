//! Tests for the shared observe surface: the reserved-path replies, the decision
//! header gating, and the config parse.

use osproxy_tenancy::TenancyRouter;

use super::*;
use crate::{Filter, ReferenceTenancy};

fn filter() -> Filter<TenancyRouter<ReferenceTenancy>> {
    Filter::new(TenancyRouter::new(ReferenceTenancy::new(
        "opensearch",
        "http://os:9200",
        "x-tenant",
    )))
}

fn headers(path: &str) -> Vec<(String, String)> {
    vec![
        (":method".to_owned(), "GET".to_owned()),
        (":path".to_owned(), path.to_owned()),
    ]
}

#[test]
fn metrics_snapshot_totals_the_outcomes() {
    let m = Metrics::default();
    m.record_routed();
    m.record_routed();
    m.record_rejected();
    assert_eq!(
        String::from_utf8(m.snapshot_json()).unwrap(),
        r#"{"requests":3,"routed":2,"rejected":1}"#
    );
}

#[test]
fn apply_query_flips_and_ignores_unknown() {
    let d = Directives::default();
    assert!(d.emit_decision());
    assert_eq!(d.apply_query("emit_decision=false&unknown=x"), 1);
    assert!(!d.emit_decision());
    assert_eq!(d.apply_query("emit_decision=maybe"), 0);
    assert!(!d.emit_decision());
}

#[test]
fn observe_config_reads_reserved_keys_and_ignores_the_rest() {
    // The tenancy keys share the blob and are ignored here.
    let cfg = ObserveConfig::from_json(
        r#"{"cluster":"os","shared_index":"s","admin_token":"s3cret","emit_decision":false}"#,
    );
    assert_eq!(cfg.admin_token.as_deref(), Some("s3cret"));
    assert!(!cfg.emit_decision);

    let bare = ObserveConfig::from_json(r#"{"cluster":"os"}"#);
    assert!(bare.admin_token.is_none());
    assert!(bare.emit_decision, "decision header defaults on");
}

#[tokio::test]
async fn metrics_path_is_answered_shape_only() {
    let observe = Observe::default();
    observe.record_routed();
    let reply = observe
        .reserved_reply(&filter(), &headers(METRICS_PATH))
        .await
        .expect("a metrics reply");
    assert_eq!(reply.status, 200);
    assert_eq!(
        String::from_utf8(reply.body).unwrap(),
        r#"{"requests":1,"routed":1,"rejected":0}"#
    );
}

#[tokio::test]
async fn explain_path_returns_a_dry_run() {
    let observe = Observe::default();
    let reply = observe
        .reserved_reply(&filter(), &headers("/_evoxy/explain/orders/_search"))
        .await
        .expect("an explain reply");
    assert_eq!(reply.status, 200);
    // Shape-only routing dry-run; no forward happened.
    assert!(String::from_utf8(reply.body).unwrap().contains("outcome"));
}

#[tokio::test]
async fn a_normal_path_is_not_reserved() {
    let observe = Observe::default();
    assert!(observe
        .reserved_reply(&filter(), &headers("/orders/_doc/1"))
        .await
        .is_none());
}

#[tokio::test]
async fn admin_plane_is_token_gated_and_flips_live() {
    let observe = Observe::from_config(&ObserveConfig {
        admin_token: Some("s3cret".to_owned()),
        emit_decision: true,
    });

    // No/ wrong bearer → 403, unchanged.
    let unauth = |auth: Option<&str>| {
        let mut h = vec![
            (":method".to_owned(), "POST".to_owned()),
            (
                ":path".to_owned(),
                format!("{ADMIN_PATH}?emit_decision=false"),
            ),
        ];
        if let Some(a) = auth {
            h.push(("authorization".to_owned(), a.to_owned()));
        }
        h
    };
    let reply = observe
        .reserved_reply(&filter(), &unauth(Some("Bearer wrong")))
        .await
        .expect("an admin reply");
    assert_eq!(reply.status, 403);

    // Correct token applies the query and echoes the flipped state.
    let reply = observe
        .reserved_reply(&filter(), &unauth(Some("Bearer s3cret")))
        .await
        .expect("an admin reply");
    assert_eq!(reply.status, 200);
    assert_eq!(
        String::from_utf8(reply.body).unwrap(),
        r#"{"emit_decision":false}"#
    );
}

#[tokio::test]
async fn admin_plane_fails_closed_without_a_configured_token() {
    let observe = Observe::default(); // no token
    let h = vec![
        (":method".to_owned(), "POST".to_owned()),
        (":path".to_owned(), ADMIN_PATH.to_owned()),
        ("authorization".to_owned(), "Bearer anything".to_owned()),
    ];
    let reply = observe
        .reserved_reply(&filter(), &h)
        .await
        .expect("an admin reply");
    assert_eq!(reply.status, 403);
}

#[tokio::test]
async fn decision_header_is_gated_by_the_directive() {
    let observe = Observe::default();
    let req = vec![
        (":method".to_owned(), "PUT".to_owned()),
        (":path".to_owned(), "/orders/_doc/1".to_owned()),
        ("x-tenant".to_owned(), "acme".to_owned()),
    ];
    // On by default → a decision shape.
    assert!(observe.decision_header(&filter(), &req).await.is_some());

    // Silence it via the directive plane; now no header.
    let silenced = Observe::from_config(&ObserveConfig {
        admin_token: None,
        emit_decision: false,
    });
    assert!(silenced.decision_header(&filter(), &req).await.is_none());
}

#[test]
fn constant_time_eq_matches_only_equal() {
    assert!(constant_time_eq(b"secret", b"secret"));
    assert!(!constant_time_eq(b"secret", b"secrez"));
    assert!(!constant_time_eq(b"secret", b"secre"));
}

/// `with_admin_token` (the ext_proc enable path) authorizes exactly like the
/// config-driven token.
#[tokio::test]
async fn with_admin_token_enables_the_plane() {
    let observe = Observe::default().with_admin_token("s3cret");
    let h = vec![
        (":method".to_owned(), "POST".to_owned()),
        (":path".to_owned(), ADMIN_PATH.to_owned()),
        ("authorization".to_owned(), "Bearer s3cret".to_owned()),
    ];
    let reply = observe
        .reserved_reply(&filter(), &h)
        .await
        .expect("an admin reply");
    assert_eq!(reply.status, 200);
}

/// An unknown `/_evoxy/` path is not a reserved surface (no accidental catch-all).
#[tokio::test]
async fn unknown_reserved_prefix_path_is_not_answered() {
    let observe = Observe::default();
    assert!(observe
        .reserved_reply(&filter(), &headers("/_evoxy/nope"))
        .await
        .is_none());
}

/// Every reserved path constant lives under the one prefix `reserved_reply` fast-
/// exits on, so a future reserved path can't be added outside it and silently fall
/// through to the data plane.
#[test]
fn reserved_paths_share_the_one_namespace() {
    assert!(METRICS_PATH.starts_with(RESERVED_PREFIX));
    assert!(ADMIN_PATH.starts_with(RESERVED_PREFIX));
    assert!(EXPLAIN_PREFIX.starts_with(RESERVED_PREFIX));
}
