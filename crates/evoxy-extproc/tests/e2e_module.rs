//! Live end-to-end correctness proof for the **dynamic-module** backend — the
//! functional twin of `e2e.rs` (which proves the ext_proc backend). It loads our
//! `.so` into a STOCK `envoyproxy/envoy:v1.37.0` (no fork, no rebuild) and drives
//! real requests through it into a real OpenSearch.
//!
//! Six tests, all `#[ignore]`'d (need Docker + the `evoxy-envoy` image; build it
//! first with `cargo xtask module-image`, then run with `--ignored`). Beyond the
//! write/read and shared-index isolation cases, they cover the config-only
//! multi-tenancy patterns: `dedicated_index_...` (per-tenant physical index),
//! `per_tenant_cluster_...` (header-matched clusters),
//! `dynamic_forward_proxy_dials_the_tenancy_endpoint` (endpoint dialed with no
//! cluster defined), and `dynamic_forward_proxy_dials_an_https_upstream` (TLS/ALB).
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

use rcgen::{CertificateParams, DnType, KeyPair};
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

/// A dynamic-forward-proxy Envoy bootstrap: NO per-upstream clusters. The module
/// rewrites `:authority` to the tenancy's endpoint (`endpoint_by_partition`), and
/// the built-in `dynamic_forward_proxy` filter + cluster dials that host. Adding an
/// upstream is a config value, not a new cluster.
fn envoy_bootstrap_dfp(filter_config: &str) -> Vec<u8> {
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
              routes: [{ match: { prefix: "/" }, route: { cluster: dfp } }]
          http_filters:
          - name: envoy.filters.http.dynamic_modules
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter
              dynamic_module_config: { name: evoxy_module }
              filter_name: evoxy
              filter_config:
                "@type": type.googleapis.com/google.protobuf.StringValue
                value: 'FILTER_CONFIG'
          - name: envoy.filters.http.dynamic_forward_proxy
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_forward_proxy.v3.FilterConfig
              dns_cache_config: { name: evoxy_dns, dns_lookup_family: V4_ONLY }
          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
  clusters:
  - name: dfp
    lb_policy: CLUSTER_PROVIDED
    cluster_type:
      name: envoy.clusters.dynamic_forward_proxy
      typed_config:
        "@type": type.googleapis.com/envoy.extensions.clusters.dynamic_forward_proxy.v3.ClusterConfig
        dns_cache_config: { name: evoxy_dns, dns_lookup_family: V4_ONLY }
"#;
    TEMPLATE
        .replace("FILTER_CONFIG", filter_config)
        .into_bytes()
}

/// A dynamic-forward-proxy bootstrap that dials the upstream over **TLS**, trusting
/// `/ca.pem` and taking SNI + cert-hostname validation from the request host
/// (`auto_sni` / `auto_san_validation`) — the AWS-ALB (HTTPS) shape. Same single
/// cluster for any HTTPS host.
fn envoy_bootstrap_dfp_tls(filter_config: &str) -> Vec<u8> {
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
              routes: [{ match: { prefix: "/" }, route: { cluster: dfp } }]
          http_filters:
          - name: envoy.filters.http.dynamic_modules
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_modules.v3.DynamicModuleFilter
              dynamic_module_config: { name: evoxy_module }
              filter_name: evoxy
              filter_config:
                "@type": type.googleapis.com/google.protobuf.StringValue
                value: 'FILTER_CONFIG'
          - name: envoy.filters.http.dynamic_forward_proxy
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.dynamic_forward_proxy.v3.FilterConfig
              dns_cache_config: { name: evoxy_dns, dns_lookup_family: V4_ONLY }
          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
  clusters:
  - name: dfp
    lb_policy: CLUSTER_PROVIDED
    cluster_type:
      name: envoy.clusters.dynamic_forward_proxy
      typed_config:
        "@type": type.googleapis.com/envoy.extensions.clusters.dynamic_forward_proxy.v3.ClusterConfig
        dns_cache_config: { name: evoxy_dns, dns_lookup_family: V4_ONLY }
    typed_extension_protocol_options:
      envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
        "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
        upstream_http_protocol_options: { auto_sni: true, auto_san_validation: true }
        explicit_http_config: { http_protocol_options: {} }
    transport_socket:
      name: envoy.transport_sockets.tls
      typed_config:
        "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext
        common_tls_context:
          validation_context:
            trusted_ca: { filename: /ca.pem }
"#;
    TEMPLATE
        .replace("FILTER_CONFIG", filter_config)
        .into_bytes()
}

/// PEM for a TLS-terminating upstream valid for `host.docker.internal`: the CA (what
/// Envoy trusts), the server chain (leaf + CA, what the terminator presents), and the
/// server key. Generated at runtime, so no secret is committed.
struct UpstreamPki {
    ca_pem: String,
    chain_pem: String,
    key_pem: String,
}

fn generate_upstream_pki() -> UpstreamPki {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "evoxy-upstream-ca");
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    // The SAN must match the request host Envoy validates against (auto_san_validation).
    let server_key = KeyPair::generate().unwrap();
    let server_params = CertificateParams::new(vec!["host.docker.internal".to_owned()]).unwrap();
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .unwrap();

    UpstreamPki {
        ca_pem: ca_cert.pem(),
        chain_pem: format!("{}{}", server_cert.pem(), ca_cert.pem()),
        key_pem: server_key.serialize_pem(),
    }
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
async fn dedicated_index_gives_each_tenant_its_own_physical_index() {
    // Config-only `dedicated_index` mode: one cluster, a per-tenant physical index
    // from `index_template`. Two tenants write the same logical `/orders/_doc/1`;
    // each lands in its own physical index (`orders-acme`, `orders-globex`), and a
    // read comes back in the logical view. No custom SPI.
    let opensearch = start_opensearch().await;
    let os_port = opensearch.get_host_port_ipv4(9200).await.unwrap();
    let config = r#"{"isolation":"dedicated_index","partition_header":"x-tenant","index_template":"orders-{partition}"}"#;
    let (_envoy, base) = start_envoy(os_port, config).await;
    let http = reqwest::Client::new();

    for (tenant, who) in [("acme", "a"), ("globex", "g")] {
        let put = http
            .put(format!("{base}/orders/_doc/1"))
            .header("x-tenant", tenant)
            .header("content-type", "application/json")
            .body(format!(r#"{{"who":"{who}"}}"#))
            .send()
            .await
            .expect("PUT through module");
        assert!(put.status().is_success(), "{tenant} write {}", put.status());
    }

    // Each tenant's doc is physically in its own index (read OpenSearch directly).
    assert_eq!(
        source_at(&http, os_port, "orders-acme", "1")
            .await
            .expect("acme doc")["who"],
        json!("a")
    );
    assert_eq!(
        source_at(&http, os_port, "orders-globex", "1")
            .await
            .expect("globex doc")["who"],
        json!("g")
    );

    // A read back through the module returns the logical index, not the physical one.
    let got: Value = http
        .get(format!("{base}/orders/_doc/1"))
        .header("x-tenant", "acme")
        .send()
        .await
        .expect("GET through module")
        .json()
        .await
        .expect("json");
    assert_eq!(
        got["_index"],
        json!("orders"),
        "logical index restored: {got}"
    );
    assert_eq!(got["_source"]["who"], json!("a"));
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + the evoxy-envoy image; run with --ignored"]
async fn dynamic_forward_proxy_dials_the_tenancy_endpoint() {
    // The osproxy-parity path: the tenancy returns a per-tenant ENDPOINT, the module
    // sets it as `:authority`, and Envoy's dynamic_forward_proxy dials it — with NO
    // cluster defined for either upstream. Two real OpenSearch backends; acme's
    // endpoint and globex's endpoint differ, so each write lands on a different one.
    let os_a = start_opensearch().await;
    let os_b = start_opensearch().await;
    let port_a = os_a.get_host_port_ipv4(9200).await.unwrap();
    let port_b = os_b.get_host_port_ipv4(9200).await.unwrap();

    // host.docker.internal is the host gateway inside the Envoy container; the two
    // backends differ only by port, which the :authority carries.
    let config = format!(
        r#"{{"partition_header":"x-tenant","endpoint_by_partition":{{"acme":"http://host.docker.internal:{port_a}","globex":"http://host.docker.internal:{port_b}"}}}}"#
    );
    let envoy = GenericImage::new(IMAGE, IMAGE_TAG)
        .with_exposed_port(ContainerPort::Tcp(10000))
        .with_wait_for(WaitFor::message_on_stderr("starting main dispatch loop"))
        .with_host("host.docker.internal", Host::HostGateway)
        .with_copy_to("/etc/envoy/envoy.yaml", envoy_bootstrap_dfp(&config))
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("evoxy-envoy starts (build it first: cargo xtask module-image)");
    let base = format!(
        "http://127.0.0.1:{}",
        envoy.get_host_port_ipv4(10000).await.unwrap()
    );
    let http = reqwest::Client::new();

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

    // Each write reached the endpoint the tenancy named — proven by reading the two
    // backends directly. No cluster was defined for either.
    assert!(
        doc_found(&http, port_a, "1").await,
        "acme dialed endpoint A"
    );
    assert!(
        !doc_found(&http, port_b, "1").await,
        "acme not on endpoint B"
    );
    assert!(
        doc_found(&http, port_b, "2").await,
        "globex dialed endpoint B"
    );
    assert!(
        !doc_found(&http, port_a, "2").await,
        "globex not on endpoint A"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + the evoxy-envoy image; run with --ignored"]
async fn dynamic_forward_proxy_dials_an_https_upstream() {
    // The AWS-ALB shape: the tenancy returns an https:// endpoint and Envoy dials it
    // over TLS via dynamic_forward_proxy, taking SNI + cert validation from the host.
    // A ghostunnel TLS terminator (serving a runtime cert for host.docker.internal)
    // fronts a real OpenSearch; Envoy trusts the generated CA. No cluster is defined
    // for the upstream.
    let opensearch = start_opensearch().await;
    let os_port = opensearch.get_host_port_ipv4(9200).await.unwrap();
    let pki = generate_upstream_pki();

    let terminator = GenericImage::new("ghostunnel/ghostunnel", "v1.8.2")
        .with_exposed_port(ContainerPort::Tcp(8443))
        .with_wait_for(WaitFor::message_on_stdout("listening for connections"))
        .with_host("host.docker.internal", Host::HostGateway)
        .with_copy_to("/server.pem", pki.chain_pem.into_bytes())
        .with_copy_to("/server-key.pem", pki.key_pem.into_bytes())
        .with_cmd([
            "server".to_owned(),
            "--listen".to_owned(),
            "0.0.0.0:8443".to_owned(),
            "--target".to_owned(),
            format!("host.docker.internal:{os_port}"),
            // The target is the host gateway, not localhost (a test harness detail).
            "--unsafe-target".to_owned(),
            "--cert".to_owned(),
            "/server.pem".to_owned(),
            "--key".to_owned(),
            "/server-key.pem".to_owned(),
            "--disable-authentication".to_owned(),
        ])
        .start()
        .await
        .expect("ghostunnel TLS terminator starts");
    let tls_port = terminator.get_host_port_ipv4(8443).await.unwrap();

    // The tenancy points acme at the HTTPS terminator (no port → the module fills 443,
    // but here the terminator is on a mapped port, so name it explicitly).
    let config = format!(
        r#"{{"partition_header":"x-tenant","endpoint_by_partition":{{"acme":"https://host.docker.internal:{tls_port}"}}}}"#
    );
    let envoy = GenericImage::new(IMAGE, IMAGE_TAG)
        .with_exposed_port(ContainerPort::Tcp(10000))
        .with_wait_for(WaitFor::message_on_stderr("starting main dispatch loop"))
        .with_host("host.docker.internal", Host::HostGateway)
        .with_copy_to("/ca.pem", pki.ca_pem.into_bytes())
        .with_copy_to("/etc/envoy/envoy.yaml", envoy_bootstrap_dfp_tls(&config))
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("evoxy-envoy starts (build it first: cargo xtask module-image)");
    let base = format!(
        "http://127.0.0.1:{}",
        envoy.get_host_port_ipv4(10000).await.unwrap()
    );
    let http = reqwest::Client::new();

    // Write through the module → TLS to the terminator → OpenSearch.
    let put = http
        .put(format!("{base}/orders/_doc/1"))
        .header("x-tenant", "acme")
        .header("content-type", "application/json")
        .body(r#"{"who":"tls"}"#)
        .send()
        .await
        .expect("PUT through module over TLS");
    let status = put.status();
    let ok = status.is_success();
    let body = put.text().await.unwrap_or_default();
    let logs = if ok {
        String::new()
    } else {
        String::from_utf8(envoy.stderr_to_vec().await.unwrap_or_default()).unwrap_or_default()
    };
    assert!(ok, "write over TLS {status}: {body}\n--- envoy ---\n{logs}");

    // It reached OpenSearch through the TLS hop.
    assert!(
        doc_found(&http, os_port, "1").await,
        "doc written over the HTTPS upstream"
    );
}

/// The `_source` of `/{index}/_doc/{id}` read directly from the OpenSearch at
/// `port`, or `None` if the document is not there.
async fn source_at(http: &reqwest::Client, port: u16, index: &str, id: &str) -> Option<Value> {
    let got: Value = http
        .get(format!("http://127.0.0.1:{port}/{index}/_doc/{id}"))
        .send()
        .await
        .expect("direct GET")
        .json()
        .await
        .expect("json");
    got["found"]
        .as_bool()
        .unwrap_or(false)
        .then(|| got["_source"].clone())
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
