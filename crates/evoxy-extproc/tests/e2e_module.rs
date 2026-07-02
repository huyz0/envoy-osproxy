//! Live end-to-end correctness proof for the **dynamic-module** backend — the
//! functional twin of `e2e.rs` (which proves the ext_proc backend). It loads our
//! `.so` into a STOCK `envoyproxy/envoy:v1.37.0` (no fork, no rebuild) and drives
//! real requests through it into a real OpenSearch.
//!
//! Three tests, all `#[ignore]`'d (need Docker + the `evoxy-envoy` image; build it
//! first with `cargo xtask module-image`, then run with `--ignored`):
//! - `write_then_read_through_module` — dedicated mode; a write and a read flow
//!   through the in-process module and round-trip.
//! - `shared_index_isolates_tenants_through_module` — shared-index mode; two
//!   tenants share one physical index, each sees only its own docs in its logical
//!   view. This is the load-bearing test of the module's request path: it exercises
//!   the header-hold + body-phase transform (physical-index path rewrite,
//!   partition-scoped id construction, `_tenant` injection) and the response
//!   reshaping — the write path a body-less GET perf run never touches.
//! - `per_tenant_cluster_routes_to_different_upstreams` — two real OpenSearch
//!   backends behind header-matched Envoy routes; the module's `x-evoxy-cluster`
//!   override sends each tenant's write to a different physical cluster.
// unwrap/expect are fine in this e2e harness; the helpers are not `#[test]` fns.
#![allow(clippy::pedantic, clippy::unwrap_used, clippy::expect_used)]
// JUSTIFY: one live harness proving the module data plane (write/read/search +
// multi-tenant isolation) through a real Envoy; shared container-setup helpers and
// two narrative tests keep the Docker cost to two container starts.

use std::time::Duration;

use serde_json::{json, Value};
use testcontainers::core::{ContainerPort, Host, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// The image built by `cargo xtask module-image`: stock Envoy + our `.so`.
const IMAGE: &str = "evoxy-envoy";
const IMAGE_TAG: &str = "v1.37.0";

/// The Envoy bootstrap: the upstream `DynamicModuleFilter` loads our `evoxy_module`
/// with the given reference-tenancy `filter_config`, then a static route to
/// OpenSearch. OpenSearch is reached over the host gateway (IPv4).
fn envoy_bootstrap(opensearch_port: u16, filter_config: &str) -> Vec<u8> {
    const TEMPLATE: &str = r#"
admin: { address: { socket_address: { address: 0.0.0.0, port_value: 9901 } } }
static_resources:
  listeners:
  - name: main
    address: { socket_address: { address: 0.0.0.0, port_value: 10000 } }
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: ingress
          route_config:
            name: local
            virtual_hosts:
            - name: all
              domains: ["*"]
              routes: [{ match: { prefix: "/" }, route: { cluster: opensearch } }]
          http_filters:
          - name: envoy.filters.http.dynamic_modules
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter
              dynamic_module_config: { name: evoxy_module }
              filter_name: evoxy
              filter_config:
                "@type": type.googleapis.com/google.protobuf.StringValue
                value: 'FILTER_CONFIG'
          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
  clusters:
  - name: opensearch
    type: STRICT_DNS
    dns_lookup_family: V4_ONLY
    load_assignment:
      cluster_name: opensearch
      endpoints:
      - lb_endpoints:
        - endpoint: { address: { socket_address: { address: host.docker.internal, port_value: OS_PORT } } }
"#;
    TEMPLATE
        .replace("OS_PORT", &opensearch_port.to_string())
        .replace("FILTER_CONFIG", filter_config)
        .into_bytes()
}

/// A two-cluster Envoy bootstrap: `x-evoxy-cluster` header-matched routes select
/// `opensearch_a` or `opensearch_b` (each a real OpenSearch), with `opensearch_a`
/// as the default. This is the config a per-request cluster override needs — the
/// module sets the header, Envoy routes on it.
fn envoy_bootstrap_multicluster(port_a: u16, port_b: u16, filter_config: &str) -> Vec<u8> {
    const TEMPLATE: &str = r#"
admin: { address: { socket_address: { address: 0.0.0.0, port_value: 9901 } } }
static_resources:
  listeners:
  - name: main
    address: { socket_address: { address: 0.0.0.0, port_value: 10000 } }
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: ingress
          route_config:
            name: local
            virtual_hosts:
            - name: all
              domains: ["*"]
              routes:
              - match: { prefix: "/", headers: [{ name: x-evoxy-cluster, string_match: { exact: opensearch_b } }] }
                route: { cluster: opensearch_b }
              - match: { prefix: "/", headers: [{ name: x-evoxy-cluster, string_match: { exact: opensearch_a } }] }
                route: { cluster: opensearch_a }
              - match: { prefix: "/" }
                route: { cluster: opensearch_a }
          http_filters:
          - name: envoy.filters.http.dynamic_modules
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter
              dynamic_module_config: { name: evoxy_module }
              filter_name: evoxy
              filter_config:
                "@type": type.googleapis.com/google.protobuf.StringValue
                value: 'FILTER_CONFIG'
          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
  clusters:
  - name: opensearch_a
    type: STRICT_DNS
    dns_lookup_family: V4_ONLY
    load_assignment:
      cluster_name: opensearch_a
      endpoints:
      - lb_endpoints:
        - endpoint: { address: { socket_address: { address: host.docker.internal, port_value: PORT_A } } }
  - name: opensearch_b
    type: STRICT_DNS
    dns_lookup_family: V4_ONLY
    load_assignment:
      cluster_name: opensearch_b
      endpoints:
      - lb_endpoints:
        - endpoint: { address: { socket_address: { address: host.docker.internal, port_value: PORT_B } } }
"#;
    TEMPLATE
        .replace("PORT_A", &port_a.to_string())
        .replace("PORT_B", &port_b.to_string())
        .replace("FILTER_CONFIG", filter_config)
        .into_bytes()
}

async fn start_opensearch() -> ContainerAsync<GenericImage> {
    GenericImage::new("opensearchproject/opensearch", "2.11.1")
        .with_exposed_port(ContainerPort::Tcp(9200))
        .with_wait_for(WaitFor::message_on_stdout("] started"))
        .with_env_var("discovery.type", "single-node")
        .with_env_var("DISABLE_SECURITY_PLUGIN", "true")
        .with_env_var("DISABLE_INSTALL_DEMO_CONFIG", "true")
        .with_env_var("bootstrap.memory_lock", "false")
        .with_env_var("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
        .start()
        .await
        .expect("opensearch starts")
}

/// Start the stock-Envoy-plus-module image over the given bootstrap; returns
/// (container, base URL). The container is returned so its logs are available.
async fn start_envoy(os_port: u16, filter_config: &str) -> (ContainerAsync<GenericImage>, String) {
    let envoy = GenericImage::new(IMAGE, IMAGE_TAG)
        .with_exposed_port(ContainerPort::Tcp(10000))
        .with_wait_for(WaitFor::message_on_stderr("starting main dispatch loop"))
        .with_host("host.docker.internal", Host::HostGateway)
        .with_copy_to(
            "/etc/envoy/envoy.yaml",
            envoy_bootstrap(os_port, filter_config),
        )
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("evoxy-envoy starts (build it first: cargo xtask module-image)");
    let port = envoy.get_host_port_ipv4(10000).await.unwrap();
    (envoy, format!("http://127.0.0.1:{port}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker + the evoxy-envoy image; run with --ignored"]
async fn write_then_read_through_module() {
    let opensearch = start_opensearch().await;
    let os_port = opensearch.get_host_port_ipv4(9200).await.unwrap();

    // Dedicated mode: index unchanged, body passed through.
    let config =
        r#"{"cluster":"opensearch","endpoint":"http://unused","partition_header":"x-tenant"}"#;
    let (_envoy, base) = start_envoy(os_port, config).await;
    let http = reqwest::Client::new();

    // Write a document THROUGH Envoy's in-process module.
    let put = http
        .put(format!("{base}/orders/_doc/42"))
        .header("x-tenant", "acme")
        .header("content-type", "application/json")
        .body(r#"{"k":1,"who":"module-e2e"}"#)
        .send()
        .await
        .expect("PUT through module");
    assert!(
        put.status().is_success(),
        "write via module: {}",
        put.status()
    );

    // Read it back THROUGH the module.
    let got: Value = http
        .get(format!("{base}/orders/_doc/42"))
        .header("x-tenant", "acme")
        .send()
        .await
        .expect("GET through module")
        .json()
        .await
        .expect("json");
    assert_eq!(got["found"], json!(true), "read via module: {got}");
    assert_eq!(got["_source"]["who"], json!("module-e2e"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker + the evoxy-envoy image; run with --ignored"]
async fn shared_index_isolates_tenants_through_module() {
    let opensearch = start_opensearch().await;
    let os_port = opensearch.get_host_port_ipv4(9200).await.unwrap();

    // Shared-index mode: both tenants share physical index `orders_shared`. This
    // makes each write a real transform — the module rewrites the path to the
    // physical index, constructs the partition-scoped id, and injects `_tenant`.
    let config = r#"{"cluster":"opensearch","endpoint":"http://unused","partition_header":"x-tenant","shared_index":"orders_shared","inject_field":"_tenant"}"#;
    let (envoy, base) = start_envoy(os_port, config).await;
    let http = reqwest::Client::new();

    // Two tenants write a doc with the SAME natural key `1` to the SAME logical
    // index; the shared-index id template scopes the physical id per tenant.
    for (tenant, who) in [("acme", "a"), ("globex", "g")] {
        let put = http
            .put(format!("{base}/orders/_doc/1"))
            .header("x-tenant", tenant)
            .header("content-type", "application/json")
            .body(format!(r#"{{"id":1,"who":"{who}"}}"#))
            .send()
            .await
            .expect("PUT through module");
        let status = put.status();
        let ok = status.is_success();
        let body = put.text().await.unwrap_or_default();
        let logs = if ok {
            String::new()
        } else {
            String::from_utf8(envoy.stderr_to_vec().await.unwrap_or_default()).unwrap_or_default()
        };
        assert!(ok, "{tenant} write {status}: {body}\n--- envoy ---\n{logs}");
    }

    // acme reads id `1` back: its own document, in its logical view (logical index
    // + id, injected `_tenant` stripped) — the response reshaping.
    let got: Value = http
        .get(format!("{base}/orders/_doc/1"))
        .header("x-tenant", "acme")
        .send()
        .await
        .expect("GET through module")
        .json()
        .await
        .expect("json");
    assert_eq!(got["_index"], json!("orders"), "logical index: {got}");
    assert_eq!(got["_id"], json!("1"), "logical id: {got}");
    assert!(
        got["_source"].get("_tenant").is_none(),
        "isolation field stripped: {got}"
    );
    assert_eq!(
        got["_source"]["who"],
        json!("a"),
        "acme sees its own doc: {got}"
    );

    // Make the writes searchable, then confirm isolation on the read path.
    http.post(format!("http://127.0.0.1:{os_port}/orders_shared/_refresh"))
        .send()
        .await
        .expect("refresh");

    let search: Value = http
        .post(format!("{base}/orders/_search"))
        .header("x-tenant", "acme")
        .header("content-type", "application/json")
        .body(r#"{"query":{"match_all":{}}}"#)
        .send()
        .await
        .expect("search through module")
        .json()
        .await
        .expect("json");
    let hits = search["hits"]["hits"].as_array().expect("hits array");
    assert_eq!(
        hits.len(),
        1,
        "acme sees only its own doc (isolation): {search}"
    );
    assert_eq!(hits[0]["_id"], json!("1"), "logical id in hit");
    assert_eq!(hits[0]["_source"]["who"], json!("a"));
    assert!(
        hits[0]["_source"].get("_tenant").is_none(),
        "injected field stripped from hit"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + the evoxy-envoy image; run with --ignored"]
async fn per_tenant_cluster_routes_to_different_upstreams() {
    // Two real OpenSearch backends behind two Envoy clusters. The reference tenancy
    // maps acme → opensearch_a and globex → opensearch_b; the module sets
    // `x-evoxy-cluster` and clears the route cache, and Envoy's header-matched routes
    // send each tenant's write to a DIFFERENT physical backend. This proves the
    // per-request cluster override the module previously did not do.
    let os_a = start_opensearch().await;
    let os_b = start_opensearch().await;
    let port_a = os_a.get_host_port_ipv4(9200).await.unwrap();
    let port_b = os_b.get_host_port_ipv4(9200).await.unwrap();

    // Dedicated mode (index unchanged), per-tenant cluster override.
    let config = r#"{"cluster":"opensearch_a","partition_header":"x-tenant","cluster_by_partition":{"acme":"opensearch_a","globex":"opensearch_b"}}"#;
    let envoy = GenericImage::new(IMAGE, IMAGE_TAG)
        .with_exposed_port(ContainerPort::Tcp(10000))
        .with_wait_for(WaitFor::message_on_stderr("starting main dispatch loop"))
        .with_host("host.docker.internal", Host::HostGateway)
        .with_copy_to(
            "/etc/envoy/envoy.yaml",
            envoy_bootstrap_multicluster(port_a, port_b, config),
        )
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("evoxy-envoy starts (build it first: cargo xtask module-image)");
    let base = format!(
        "http://127.0.0.1:{}",
        envoy.get_host_port_ipv4(10000).await.unwrap()
    );
    let http = reqwest::Client::new();

    // Each tenant writes through the module; routing is decided by tenant → cluster.
    for (tenant, id, who) in [("acme", "1", "a"), ("globex", "2", "g")] {
        let put = http
            .put(format!("{base}/orders/_doc/{id}"))
            .header("x-tenant", tenant)
            .header("content-type", "application/json")
            .body(format!(r#"{{"who":"{who}"}}"#))
            .send()
            .await
            .expect("PUT through module");
        let status = put.status();
        let ok = status.is_success();
        let body = put.text().await.unwrap_or_default();
        let logs = if ok {
            String::new()
        } else {
            String::from_utf8(envoy.stderr_to_vec().await.unwrap_or_default()).unwrap_or_default()
        };
        assert!(ok, "{tenant} write {status}: {body}\n--- envoy ---\n{logs}");
    }

    // Read each backend DIRECTLY (not through Envoy): acme's doc must be in A only,
    // globex's in B only. That is the routing proof — different tenants, different
    // physical clusters.
    assert!(
        doc_found(&http, port_a, "1").await,
        "acme's doc is in cluster A"
    );
    assert!(
        !doc_found(&http, port_b, "1").await,
        "acme's doc is NOT in cluster B"
    );
    assert!(
        doc_found(&http, port_b, "2").await,
        "globex's doc is in cluster B"
    );
    assert!(
        !doc_found(&http, port_a, "2").await,
        "globex's doc is NOT in cluster A"
    );
}

/// Whether `/orders/_doc/{id}` exists directly on the OpenSearch at `port`.
async fn doc_found(http: &reqwest::Client, port: u16, id: &str) -> bool {
    let got: Value = http
        .get(format!("http://127.0.0.1:{port}/orders/_doc/{id}"))
        .send()
        .await
        .expect("direct GET")
        .json()
        .await
        .expect("json");
    got["found"].as_bool().unwrap_or(false)
}
