//! Live mTLS proof (M4b): a real client certificate → Envoy-terminated mTLS →
//! `x-forwarded-client-cert` → the principal the tenancy keys on, end to end
//! through a stock Envoy. `#[ignore]`'d (needs a Docker daemon; run `--ignored`).
//!
//! The reference tenancy runs in shared-index mode with `partition_from_principal`
//! and `require_mtls_for_mutation`, so this exercises the whole M4 chain: Envoy
//! validates the client cert and sets XFCC; `convert` parses it into the identity;
//! the mTLS-for-mutation policy admits the write (a cert was presented); and the
//! partition is the **Envoy-validated SPIFFE principal** (the cert's URI SAN), not
//! a client header — so the stored physical doc carries `_tenant = spiffe://td/acme`
//! (its slashes percent-encoded in the doc-id path, decoded by OpenSearch).
//!
//! Certificates (CA + server + client) are generated at runtime with `rcgen`, so
//! nothing secret is committed. The client→Envoy leg is TLS; Envoy re-originates
//! plaintext to OpenSearch (the upstream leg is unchanged).
// unwrap/expect are fine in this e2e harness; the helpers are not `#[test]` fns.
#![allow(clippy::pedantic, clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use evoxy_extproc::{ExtProcService, ExternalProcessorServer};
use evoxy_filter::{Filter, FilterConfig, ReferenceTenancy};
use osproxy_tenancy::TenancyRouter;
use rcgen::{CertificateParams, DnType, Ia5String, KeyPair, SanType};
use serde_json::{json, Value};
use testcontainers::core::{ContainerPort, Host, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio_stream::wrappers::TcpListenerStream;

/// The SPIFFE identity the client certificate carries (its URI SAN) — also the
/// tenant the principal resolves to. Its slashes are percent-encoded in the
/// doc-id path (evoxy-route::encode) and decoded by OpenSearch.
const CLIENT_PRINCIPAL: &str = "spiffe://td/acme";

/// A generated PKI: PEM bytes for the pieces each party needs.
struct Pki {
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    /// Client cert + key concatenated (reqwest `Identity::from_pem` wants both).
    client_identity_pem: String,
}

/// Generate a CA, a server cert (for the Envoy listener, valid for `127.0.0.1`),
/// and a client cert whose URI SAN is [`CLIENT_PRINCIPAL`].
fn generate_pki() -> Pki {
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "evoxy-test-ca");
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    // Server cert: SAN must match the connect host (127.0.0.1).
    let server_key = KeyPair::generate().unwrap();
    let mut server_params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    server_params
        .subject_alt_names
        .push(SanType::IpAddress("127.0.0.1".parse().unwrap()));
    let server_cert = server_params
        .signed_by(&server_key, &ca_cert, &ca_key)
        .unwrap();

    // Client cert: a SPIFFE URI SAN is the identity Envoy forwards as XFCC and our
    // `stable_id` prefers.
    let client_key = KeyPair::generate().unwrap();
    let mut client_params = CertificateParams::new(Vec::new()).unwrap();
    client_params
        .distinguished_name
        .push(DnType::CommonName, "acme-client");
    client_params
        .subject_alt_names
        .push(SanType::URI(Ia5String::try_from(CLIENT_PRINCIPAL).unwrap()));
    let client_cert = client_params
        .signed_by(&client_key, &ca_cert, &ca_key)
        .unwrap();

    Pki {
        ca_pem: ca_cert.pem(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
        client_identity_pem: format!("{}{}", client_cert.pem(), client_key.serialize_pem()),
    }
}

/// The Envoy bootstrap with a **downstream mTLS** listener: it requires a client
/// certificate, validates it against the test CA, and forwards the validated
/// identity as XFCC (`SANITIZE_SET` + URI/subject details) to our ext_proc filter.
fn envoy_bootstrap(extproc_port: u16, opensearch_port: u16) -> Vec<u8> {
    const TEMPLATE: &str = r#"
admin:
  address: { socket_address: { address: 0.0.0.0, port_value: 9901 } }
static_resources:
  listeners:
  - name: main
    address: { socket_address: { address: 0.0.0.0, port_value: 10000 } }
    filter_chains:
    - transport_socket:
        name: envoy.transport_sockets.tls
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext
          require_client_certificate: true
          common_tls_context:
            tls_certificates:
            - certificate_chain: { filename: /etc/envoy/server.crt }
              private_key: { filename: /etc/envoy/server.key }
            validation_context:
              trusted_ca: { filename: /etc/envoy/ca.crt }
      filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: ingress
          forward_client_cert_details: SANITIZE_SET
          set_current_client_cert_details: { uri: true, subject: true }
          route_config:
            name: local
            virtual_hosts:
            - name: all
              domains: ["*"]
              routes:
              - match: { prefix: "/" }
                route: { cluster: opensearch }
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

fn spawn_service<R: osproxy_tenancy::Router>(service: ExtProcService<R>) -> u16 {
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

async fn start_envoy(
    pki: &Pki,
    svc_port: u16,
    os_port: u16,
) -> (ContainerAsync<GenericImage>, String) {
    let envoy = GenericImage::new("envoyproxy/envoy", "v1.31-latest")
        .with_exposed_port(ContainerPort::Tcp(10000))
        .with_wait_for(WaitFor::message_on_stderr("starting main dispatch loop"))
        .with_host("host.docker.internal", Host::HostGateway)
        .with_copy_to("/etc/envoy/envoy.yaml", envoy_bootstrap(svc_port, os_port))
        .with_copy_to("/etc/envoy/ca.crt", pki.ca_pem.clone().into_bytes())
        .with_copy_to(
            "/etc/envoy/server.crt",
            pki.server_cert_pem.clone().into_bytes(),
        )
        .with_copy_to(
            "/etc/envoy/server.key",
            pki.server_key_pem.clone().into_bytes(),
        )
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("envoy starts");
    let port = envoy.get_host_port_ipv4(10000).await.unwrap();
    (envoy, format!("https://127.0.0.1:{port}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with --ignored"]
async fn mtls_principal_drives_tenancy() {
    let pki = generate_pki();
    let opensearch = start_opensearch().await;
    let os_port = opensearch.get_host_port_ipv4(9200).await.unwrap();

    // Shared-index mode, partition FROM the mTLS principal, writes REQUIRE mTLS.
    let config = FilterConfig {
        cluster: "opensearch".to_owned(),
        cluster_by_partition: Default::default(),
        endpoint_by_partition: Default::default(),
        endpoint: "http://unused".to_owned(),
        partition_header: "x-tenant".to_owned(),
        shared_index: Some("orders_shared".to_owned()),
        inject_field: "_tenant".to_owned(),
        partition_from_principal: true,
    };
    let filter = Filter::new(TenancyRouter::new(ReferenceTenancy::from_config(&config)))
        .with_require_mtls_for_mutation(true);
    let svc_port = spawn_service(ExtProcService::new(filter));
    let (envoy, base) = start_envoy(&pki, svc_port, os_port).await;

    // A client that presents the acme certificate and trusts the test CA.
    let identity = reqwest::Identity::from_pem(pki.client_identity_pem.as_bytes()).unwrap();
    let ca = reqwest::Certificate::from_pem(pki.ca_pem.as_bytes()).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(ca)
        .identity(identity)
        .build()
        .unwrap();

    // Write over mTLS — no `x-tenant` header at all; the partition comes from the
    // certificate. The mTLS-for-mutation policy admits it (a cert was presented).
    let put = client
        .put(format!("{base}/orders/_doc/1"))
        .header("content-type", "application/json")
        .body(r#"{"id":1,"who":"acme"}"#)
        .send()
        .await
        .expect("mTLS PUT through Envoy");
    let status = put.status();
    let body = put.text().await.unwrap_or_default();
    let logs = if status.is_success() {
        String::new()
    } else {
        String::from_utf8(envoy.stderr_to_vec().await.unwrap_or_default()).unwrap_or_default()
    };
    assert!(status.is_success(), "mTLS write {status}: {body}\n{logs}");

    // Read it back over mTLS: logical view, injected `_tenant` stripped.
    let got: Value = client
        .get(format!("{base}/orders/_doc/1"))
        .send()
        .await
        .expect("mTLS GET through Envoy")
        .json()
        .await
        .expect("json");
    assert_eq!(got["_index"], json!("orders"), "logical index: {got}");
    assert_eq!(got["_id"], json!("1"), "logical id: {got}");
    assert!(
        got["_source"].get("_tenant").is_none(),
        "isolation field stripped: {got}"
    );

    // The decisive check: query OpenSearch DIRECTLY (plaintext, bypassing the
    // proxy) and confirm the stored physical doc's partition is the Envoy-validated
    // SPIFFE principal — proving the mTLS identity, not a client header, drove
    // tenancy.
    let direct = reqwest::Client::new();
    direct
        .post(format!("http://127.0.0.1:{os_port}/orders_shared/_refresh"))
        .send()
        .await
        .expect("refresh");
    let raw: Value = direct
        .post(format!("http://127.0.0.1:{os_port}/orders_shared/_search"))
        .header("content-type", "application/json")
        .body(r#"{"query":{"match_all":{}}}"#)
        .send()
        .await
        .expect("direct search")
        .json()
        .await
        .expect("json");
    let hits = raw["hits"]["hits"].as_array().expect("hits");
    assert_eq!(hits.len(), 1, "one stored doc: {raw}");
    assert_eq!(
        hits[0]["_source"]["_tenant"],
        json!(CLIENT_PRINCIPAL),
        "the physical partition is the mTLS principal: {raw}"
    );
    // And the physical id is partition-scoped by that same principal.
    assert_eq!(
        hits[0]["_id"],
        json!(format!("{CLIENT_PRINCIPAL}:1")),
        "scoped id: {raw}"
    );
}
