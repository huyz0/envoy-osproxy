//! Live end-to-end proof of the whole thesis: a stock Envoy + our ext_proc
//! service (no Envoy rebuild) routes and transforms a real write into a real
//! OpenSearch.
//!
//! Topology (all reachable via the host gateway, so no shared docker network):
//! ```text
//!   reqwest ─PUT /orders/_doc/42─► Envoy(container) ─ext_proc gRPC─► our service(host)
//!                                        │  runs the reused brain, mutates the body
//!                                        └─forwards───────────────► OpenSearch(container)
//! ```
//! The service runs the reference tenancy (every partition on the `opensearch`
//! cluster). M1 routes statically to that one cluster; the filter still sets
//! `x-evoxy-cluster` for the M2 multi-cluster path. We then read the doc straight
//! from OpenSearch (realtime GET) and assert it landed.
//!
//! `#[ignore]`'d — needs a Docker daemon; run with `--ignored`.
#![allow(clippy::pedantic)]

use std::time::Duration;

use evoxy_extproc::{ExtProcService, ExternalProcessorServer};
use evoxy_filter::{Filter, ReferenceTenancy};
use osproxy_tenancy::TenancyRouter;
use testcontainers::core::{ContainerPort, Host, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio_stream::wrappers::TcpListenerStream;

/// The Envoy bootstrap: an ext_proc HTTP filter (buffered request body) calling
/// our service, and a header-matched route to the OpenSearch cluster. Both
/// upstreams are reached over the host gateway.
fn envoy_bootstrap(extproc_port: u16, opensearch_port: u16) -> Vec<u8> {
    const TEMPLATE: &str = r#"
admin:
  address: { socket_address: { address: 0.0.0.0, port_value: 9901 } }
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
              # M1 is single-cluster (the reference tenancy resolves one cluster),
              # so route statically to it. Our filter still sets `x-evoxy-cluster`;
              # header-based multi-cluster selection needs header-phase re-routing
              # (a body-phase header mutation does not reliably re-route) and lands
              # with M2. This test proves the data-plane thesis: a write flows
              # through stock Envoy + our ext_proc service into real OpenSearch.
              - match: { prefix: "/" }
                route: { cluster: opensearch }
          http_filters:
          - name: envoy.filters.http.ext_proc
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
              grpc_service: { envoy_grpc: { cluster_name: extproc } }
              # Permit the filter to rewrite routing pseudo-headers (:path,
              # :method, :authority) — our transform rewrites the path/index.
              mutation_rules: { allow_all_routing: true, allow_envoy: true }
              processing_mode:
                request_header_mode: SEND
                request_body_mode: BUFFERED
                response_header_mode: SKIP
                response_body_mode: NONE
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
  - name: extproc
    type: STRICT_DNS
    dns_lookup_family: V4_ONLY
    typed_extension_protocol_options:
      envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
        "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
        explicit_http_config: { http2_protocol_options: {} }
    load_assignment:
      cluster_name: extproc
      endpoints:
      - lb_endpoints:
        - endpoint: { address: { socket_address: { address: host.docker.internal, port_value: SVC_PORT } } }
"#;
    TEMPLATE
        .replace("OS_PORT", &opensearch_port.to_string())
        .replace("SVC_PORT", &extproc_port.to_string())
        .into_bytes()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with --ignored"]
async fn write_through_envoy_lands_in_opensearch() {
    // 1. Real OpenSearch (security disabled), reachable on a host port.
    let opensearch = GenericImage::new("opensearchproject/opensearch", "2.11.1")
        .with_exposed_port(ContainerPort::Tcp(9200))
        .with_wait_for(WaitFor::message_on_stdout("] started"))
        .with_env_var("discovery.type", "single-node")
        .with_env_var("DISABLE_SECURITY_PLUGIN", "true")
        .with_env_var("DISABLE_INSTALL_DEMO_CONFIG", "true")
        .with_env_var("bootstrap.memory_lock", "false")
        .with_env_var("OPENSEARCH_JAVA_OPTS", "-Xms512m -Xmx512m")
        .start()
        .await
        .expect("opensearch starts");
    let os_port = opensearch.get_host_port_ipv4(9200).await.unwrap();

    // 2. Our ext_proc service on an ephemeral host port (reference tenancy).
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", 0)).await.unwrap();
    let svc_port = listener.local_addr().unwrap().port();
    let filter = Filter::new(TenancyRouter::new(ReferenceTenancy::new(
        "opensearch",
        "http://unused", // Envoy forwards; the endpoint on the placement is unused here.
        "x-tenant",
    )));
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(ExternalProcessorServer::new(ExtProcService::new(filter)))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .ok();
    });

    // 3. Stock Envoy loading a bootstrap that ext_proc's to us and routes on the
    //    cluster header. Both upstreams via the host gateway.
    let envoy = GenericImage::new("envoyproxy/envoy", "v1.31-latest")
        .with_exposed_port(ContainerPort::Tcp(10000))
        .with_wait_for(WaitFor::message_on_stderr("starting main dispatch loop"))
        .with_host("host.docker.internal", Host::HostGateway)
        .with_copy_to("/etc/envoy/envoy.yaml", envoy_bootstrap(svc_port, os_port))
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("envoy starts");
    let envoy_host = envoy.get_host().await.unwrap();
    let envoy_port = envoy.get_host_port_ipv4(10000).await.unwrap();

    // 4. Write a document THROUGH Envoy (which calls our filter, which routes it).
    let http = reqwest::Client::new();
    let put = http
        .put(format!("http://{envoy_host}:{envoy_port}/orders/_doc/42"))
        .header("x-tenant", "acme")
        .header("content-type", "application/json")
        .header("x-request-id", "e2e-1")
        .body(r#"{"k":1,"who":"e2e"}"#)
        .send()
        .await
        .expect("PUT through Envoy");
    let status = put.status();
    let ok = status.is_success();
    let body = put.text().await.unwrap_or_default();
    let logs = if ok {
        String::new()
    } else {
        String::from_utf8(envoy.stderr_to_vec().await.unwrap_or_default()).unwrap_or_default()
    };
    assert!(
        ok,
        "write via Envoy did not succeed: {status} body={body}\n--- envoy stderr ---\n{logs}"
    );

    // 5. Read it straight from OpenSearch (realtime GET) — it must have landed.
    let got: serde_json::Value = http
        .get(format!("http://127.0.0.1:{os_port}/orders/_doc/42"))
        .send()
        .await
        .expect("GET from OpenSearch")
        .json()
        .await
        .expect("json");
    assert_eq!(
        got["found"],
        serde_json::Value::Bool(true),
        "doc not found: {got}"
    );
    assert_eq!(got["_source"]["k"], serde_json::json!(1));
    assert_eq!(got["_source"]["who"], serde_json::json!("e2e"));

    // 6. Read the same document back THROUGH Envoy (the M2 read path: the GET
    //    flows through our filter and is forwarded to OpenSearch).
    let via_envoy: serde_json::Value = http
        .get(format!("http://{envoy_host}:{envoy_port}/orders/_doc/42"))
        .header("x-tenant", "acme")
        .send()
        .await
        .expect("GET through Envoy")
        .json()
        .await
        .expect("json");
    assert_eq!(
        via_envoy["found"],
        serde_json::Value::Bool(true),
        "read via Envoy: {via_envoy}"
    );
    assert_eq!(via_envoy["_source"]["who"], serde_json::json!("e2e"));
}
