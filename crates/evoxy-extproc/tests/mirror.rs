//! Async fan-out mechanism proof (ADR-005): Envoy `request_mirror_policies`
//! shadows the **ext_proc-transformed** request to a bridge cluster.
//!
//! `#[ignore]`'d (needs Docker; run `--ignored`). ADR-005 decides async fan-out is
//! Envoy request-mirroring to a dedicated HTTP→Kafka bridge, *not* an in-filter
//! Kafka produce (an extension can't cleanly produce to Kafka). This proves the
//! architecturally-novel half live: a write flows through stock Envoy + our filter
//! into OpenSearch (the primary), and Envoy **mirrors** the request — as the filter
//! transformed it (physical index, partition-scoped id, injected tenancy field) —
//! to a second cluster, here a recording bridge standing in for the Kafka producer.
//! The producer itself is a documented seam (osproxy's `krafka`/async-write); what
//! matters is that the bridge receives the *physical* request, fire-and-forget.
// unwrap/expect are fine in this harness; the helpers are not `#[test]` fns.
#![allow(clippy::pedantic, clippy::unwrap_used, clippy::expect_used)]
// JUSTIFY: one self-contained live harness — the mirror-enabled Envoy bootstrap,
// the recording-bridge server, and the fan-out assertion belong together.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use evoxy_extproc::{ExtProcService, ExternalProcessorServer};
use evoxy_filter::{Filter, FilterConfig, ReferenceTenancy};
use osproxy_tenancy::TenancyRouter;
use testcontainers::core::{ContainerPort, Host, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

/// The Envoy bootstrap with a **request mirror policy**: the main route forwards
/// to OpenSearch and *shadows* every request to the `mirror` (bridge) cluster.
fn envoy_bootstrap(extproc_port: u16, opensearch_port: u16, bridge_port: u16) -> Vec<u8> {
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
              - match: { prefix: "/" }
                route:
                  cluster: opensearch
                  request_mirror_policies:
                  - cluster: mirror
          http_filters:
          - name: envoy.filters.http.ext_proc
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
              grpc_service: { envoy_grpc: { cluster_name: extproc } }
              mutation_rules: { allow_all_routing: true, allow_envoy: true }
              processing_mode:
                request_header_mode: SEND
                request_body_mode: BUFFERED
                response_header_mode: SEND
                response_body_mode: BUFFERED
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
  - name: mirror
    type: STRICT_DNS
    dns_lookup_family: V4_ONLY
    load_assignment:
      cluster_name: mirror
      endpoints:
      - lb_endpoints:
        - endpoint: { address: { socket_address: { address: host.docker.internal, port_value: BRIDGE_PORT } } }
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
        .replace("BRIDGE_PORT", &bridge_port.to_string())
        .replace("SVC_PORT", &extproc_port.to_string())
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

fn spawn_service(service: ExtProcService) -> u16 {
    let listener = std::net::TcpListener::bind(("0.0.0.0", 0)).expect("bind");
    listener.set_nonblocking(true).expect("nonblocking");
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
        tonic::transport::Server::builder()
            .add_service(ExternalProcessorServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .ok();
    });
    port
}

/// A minimal HTTP/1.1 recording "bridge": it accepts a mirrored request, reads it,
/// records the raw bytes (so the assertion is robust to chunked encoding), and
/// answers `200`. It stands in for the HTTP→Kafka producer (ADR-005): the point is
/// that Envoy delivers the transformed request here, not that we produce.
async fn spawn_bridge() -> (u16, Arc<Mutex<Vec<String>>>) {
    let listener = TcpListener::bind(("0.0.0.0", 0))
        .await
        .expect("bridge bind");
    let port = listener.local_addr().unwrap().port();
    let records: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = records.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                continue;
            };
            let sink = sink.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut tmp = [0u8; 4096];
                // Read whatever the mirror sends within a short window; a mirror is
                // one request per connection, so a brief idle means "done".
                loop {
                    match tokio::time::timeout(Duration::from_millis(300), sock.read(&mut tmp))
                        .await
                    {
                        Ok(Ok(0)) | Err(_) => break,
                        Ok(Ok(n)) => buf.extend_from_slice(&tmp[..n]),
                        Ok(Err(_)) => break,
                    }
                    if buf.len() > 64 {
                        break;
                    }
                }
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                    .await;
                if !buf.is_empty() {
                    sink.lock()
                        .unwrap()
                        .push(String::from_utf8_lossy(&buf).into_owned());
                }
            });
        }
    });
    (port, records)
}

async fn start_envoy(
    svc_port: u16,
    os_port: u16,
    bridge_port: u16,
) -> (ContainerAsync<GenericImage>, String) {
    let envoy = GenericImage::new("envoyproxy/envoy", "v1.31-latest")
        .with_exposed_port(ContainerPort::Tcp(10000))
        .with_wait_for(WaitFor::message_on_stderr("starting main dispatch loop"))
        .with_host("host.docker.internal", Host::HostGateway)
        .with_copy_to(
            "/etc/envoy/envoy.yaml",
            envoy_bootstrap(svc_port, os_port, bridge_port),
        )
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("envoy starts");
    let port = envoy.get_host_port_ipv4(10000).await.unwrap();
    (envoy, format!("http://127.0.0.1:{port}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with --ignored"]
async fn envoy_mirrors_the_transformed_request_to_the_bridge() {
    let opensearch = start_opensearch().await;
    let os_port = opensearch.get_host_port_ipv4(9200).await.unwrap();

    // Shared-index mode, so the write is visibly transformed: physical index
    // `orders_shared`, partition-scoped id `acme:1`, injected `_tenant`.
    let config = FilterConfig {
        cluster: "opensearch".to_owned(),
        endpoint: "http://unused".to_owned(),
        partition_header: "x-tenant".to_owned(),
        shared_index: Some("orders_shared".to_owned()),
        inject_field: "_tenant".to_owned(),
        partition_from_principal: false,
    };
    let filter = Filter::new(TenancyRouter::new(ReferenceTenancy::from_config(&config)));
    let svc_port = spawn_service(ExtProcService::new(filter));
    let (bridge_port, records) = spawn_bridge().await;
    let (_envoy, base) = start_envoy(svc_port, os_port, bridge_port).await;
    let http = reqwest::Client::new();

    // Write through Envoy: the primary lands in OpenSearch, and Envoy mirrors the
    // request to the bridge (fire-and-forget).
    let put = http
        .put(format!("{base}/orders/_doc/1"))
        .header("x-tenant", "acme")
        .header("content-type", "application/json")
        .body(r#"{"id":1,"who":"acme"}"#)
        .send()
        .await
        .expect("write through Envoy");
    assert!(put.status().is_success(), "primary write: {}", put.status());

    // The mirror is async; poll the bridge briefly for it.
    let mut mirrored = String::new();
    for _ in 0..40 {
        if let Some(rec) = records.lock().unwrap().first() {
            mirrored = rec.clone();
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        !mirrored.is_empty(),
        "the bridge received a mirrored request"
    );
    // The mirror carries the request AS THE FILTER TRANSFORMED IT: the physical
    // index + partition-scoped, percent-encoded id in the path, and the injected
    // tenancy field in the body — not the client's logical request.
    assert!(
        mirrored.contains("/orders_shared/_doc/acme%3A1"),
        "mirror has the transformed path:\n{mirrored}"
    );
    assert!(
        mirrored.contains("_tenant") && mirrored.contains("acme"),
        "mirror body has the injected tenancy field:\n{mirrored}"
    );
}
