//! Live end-to-end proof: a stock Envoy + our ext_proc service (no Envoy rebuild)
//! routes and transforms real requests against a real OpenSearch.
//!
//! Two tests, both `#[ignore]`'d (need a Docker daemon; run with `--ignored`):
//! - `write_then_read_through_envoy` — dedicated mode; a write and a read flow
//!   through stock Envoy into OpenSearch and round-trip.
//! - `shared_index_isolates_tenants` — shared-index mode; two tenants share one
//!   physical index, and each sees only its own documents, in its logical view
//!   (partition-scoped ids constructed on write and mapped back on read, the
//!   isolation field injected then stripped, the query partition-filtered).
//!
//! Both upstreams (OpenSearch, our service) are reached from the Envoy container
//! via the host gateway, so no shared docker network is needed.
// unwrap/expect are fine in this e2e harness; the helpers below are not `#[test]`
// fns, so `allow-unwrap-in-tests` does not cover them.
#![allow(clippy::pedantic, clippy::unwrap_used, clippy::expect_used)]
// JUSTIFY: one live harness proving the full data plane (write/read/search/bulk/
// mget/msearch + multi-tenant isolation) end-to-end through a real Envoy; the
// shared container-setup helpers and a single narrative isolation test keep the
// Docker cost to two container starts, so splitting would duplicate the fixtures.

use std::time::Duration;

use evoxy_extproc::{ExtProcService, ExternalProcessorServer};
use evoxy_filter::{Filter, FilterConfig, ReferenceTenancy};
use osproxy_tenancy::TenancyRouter;
use serde_json::{json, Value};
use testcontainers::core::{ContainerPort, Host, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio_stream::wrappers::TcpListenerStream;

/// The Envoy bootstrap: an ext_proc HTTP filter (buffered request + response
/// body) calling our service, and a static route to the OpenSearch cluster. Both
/// upstreams are reached over the host gateway (IPv4).
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

/// Start a single-node OpenSearch (security disabled).
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

/// Serve an ext_proc service on an ephemeral host port; returns the port.
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

/// Start a stock Envoy over the generated bootstrap; returns (container, base URL).
async fn start_envoy(svc_port: u16, os_port: u16) -> (ContainerAsync<GenericImage>, String) {
    let envoy = GenericImage::new("envoyproxy/envoy", "v1.31-latest")
        .with_exposed_port(ContainerPort::Tcp(10000))
        .with_wait_for(WaitFor::message_on_stderr("starting main dispatch loop"))
        .with_host("host.docker.internal", Host::HostGateway)
        .with_copy_to("/etc/envoy/envoy.yaml", envoy_bootstrap(svc_port, os_port))
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("envoy starts");
    let host = envoy.get_host().await.unwrap();
    let port = envoy.get_host_port_ipv4(10000).await.unwrap();
    (envoy, format!("http://{host}:{port}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with --ignored"]
async fn write_then_read_through_envoy() {
    let opensearch = start_opensearch().await;
    let os_port = opensearch.get_host_port_ipv4(9200).await.unwrap();

    let filter = Filter::new(TenancyRouter::new(ReferenceTenancy::new(
        "opensearch",
        "http://unused",
        "x-tenant",
    )));
    // A small request-body cap so the bounded-memory guard is exercised live: the
    // ~19-byte write below passes; a larger body is refused with `413`.
    let svc_port = spawn_service(ExtProcService::new(filter).with_max_request_body_bytes(64));
    let (_envoy, base) = start_envoy(svc_port, os_port).await;
    let http = reqwest::Client::new();

    // Write a document THROUGH Envoy.
    let put = http
        .put(format!("{base}/orders/_doc/42"))
        .header("x-tenant", "acme")
        .header("content-type", "application/json")
        .body(r#"{"k":1,"who":"e2e"}"#)
        .send()
        .await
        .expect("PUT through Envoy");
    assert!(
        put.status().is_success(),
        "write via Envoy: {}",
        put.status()
    );

    // Read it back THROUGH Envoy.
    let got: Value = http
        .get(format!("{base}/orders/_doc/42"))
        .header("x-tenant", "acme")
        .send()
        .await
        .expect("GET through Envoy")
        .json()
        .await
        .expect("json");
    assert_eq!(got["found"], json!(true), "read via Envoy: {got}");
    assert_eq!(got["_source"]["who"], json!("e2e"));

    // M3d: a request body over the service's cap is refused with `413` THROUGH
    // Envoy — the bounded-memory guard fails closed before the brain buffers it.
    let big = format!(r#"{{"k":1,"pad":"{}"}}"#, "x".repeat(128));
    let too_large = http
        .put(format!("{base}/orders/_doc/99"))
        .header("x-tenant", "acme")
        .header("content-type", "application/json")
        .body(big)
        .send()
        .await
        .expect("oversized PUT through Envoy");
    assert_eq!(
        too_large.status().as_u16(),
        413,
        "oversized body refused: {}",
        too_large.status()
    );
    let err: Value = too_large.json().await.expect("413 json body");
    assert_eq!(
        err["error"],
        json!("payload_too_large"),
        "shape-only 413: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker; run with --ignored"]
async fn shared_index_isolates_tenants() {
    let opensearch = start_opensearch().await;
    let os_port = opensearch.get_host_port_ipv4(9200).await.unwrap();

    // Shared-index mode: both tenants share physical index `orders_shared`.
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
    let (envoy, base) = start_envoy(svc_port, os_port).await;
    let http = reqwest::Client::new();

    // Two tenants write a doc with the SAME natural key `id:1` to the SAME
    // logical index. The shared-index id template `{partition}:{body.id}` scopes
    // the physical id per tenant (acme:1 vs globex:1).
    for (tenant, who) in [("acme", "a"), ("globex", "g")] {
        let put = http
            .put(format!("{base}/orders/_doc/1"))
            .header("x-tenant", tenant)
            .header("content-type", "application/json")
            .body(format!(r#"{{"id":1,"who":"{who}"}}"#))
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
        assert!(ok, "{tenant} write {status}: {body}\n--- envoy ---\n{logs}");
    }

    // acme reads id `1` back: its own document, in its logical view (logical
    // index + id, no injected `_tenant`).
    let got: Value = http
        .get(format!("{base}/orders/_doc/1"))
        .header("x-tenant", "acme")
        .send()
        .await
        .expect("GET through Envoy")
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

    // Make the writes searchable.
    http.post(format!("http://127.0.0.1:{os_port}/orders_shared/_refresh"))
        .send()
        .await
        .expect("refresh");

    // acme searches: it sees ONLY its own document (isolation), logical view.
    let search: Value = http
        .post(format!("{base}/orders/_search"))
        .header("x-tenant", "acme")
        .header("content-type", "application/json")
        .body(r#"{"query":{"match_all":{}}}"#)
        .send()
        .await
        .expect("search through Envoy")
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

    // acme bulk-writes two more docs THROUGH Envoy (M3: NDJSON rewritten in place
    // — each item injected + partition-scoped id + physical index).
    let bulk = "{\"index\":{}}\n{\"id\":10,\"who\":\"a10\"}\n{\"index\":{}}\n{\"id\":11,\"who\":\"a11\"}\n";
    let resp = http
        .post(format!("{base}/orders/_bulk"))
        .header("x-tenant", "acme")
        .header("content-type", "application/x-ndjson")
        .body(bulk)
        .send()
        .await
        .expect("bulk through Envoy");
    assert!(resp.status().is_success(), "bulk write: {}", resp.status());

    // M3b: the bulk RESPONSE is reshaped back to the client's logical view — each
    // item reports the logical `_index` and the logical `_id` (physical→logical),
    // never the partition-scoped physical id.
    let bulk_body: Value = resp.json().await.expect("bulk response json");
    let items = bulk_body["items"].as_array().expect("items array");
    assert_eq!(items.len(), 2, "two bulk items: {bulk_body}");
    let item_ids: Vec<&str> = items
        .iter()
        .filter_map(|it| it["index"]["_id"].as_str())
        .collect();
    assert_eq!(item_ids, vec!["10", "11"], "logical ids in bulk response");
    for it in items {
        assert_eq!(
            it["index"]["_index"],
            json!("orders"),
            "logical index in bulk item: {it}"
        );
    }

    http.post(format!("http://127.0.0.1:{os_port}/orders_shared/_refresh"))
        .send()
        .await
        .expect("refresh");

    // acme now has 3 docs (1, 10, 11), all in its logical view, still isolated.
    let after: Value = http
        .post(format!("{base}/orders/_search"))
        .header("x-tenant", "acme")
        .header("content-type", "application/json")
        .body(r#"{"query":{"match_all":{}},"size":20}"#)
        .send()
        .await
        .expect("search through Envoy")
        .json()
        .await
        .expect("json");
    let after_hits = after["hits"]["hits"].as_array().expect("hits array");
    assert_eq!(
        after_hits.len(),
        3,
        "acme sees its 3 docs after bulk: {after}"
    );
    let ids: Vec<&str> = after_hits
        .iter()
        .filter_map(|h| h["_id"].as_str())
        .collect();
    assert!(
        ids.contains(&"1") && ids.contains(&"10") && ids.contains(&"11"),
        "logical bulk ids present: {ids:?}"
    );

    // M3b: `_mget` demux THROUGH Envoy — acme fetches its own ids 1 and 10 by
    // logical id; each comes back in the logical view (logical id, `_tenant`
    // stripped). globex's identical key `1` is a *different* physical doc, so this
    // never crosses tenants.
    let mget: Value = http
        .post(format!("{base}/orders/_mget"))
        .header("x-tenant", "acme")
        .header("content-type", "application/json")
        .body(r#"{"ids":["1","10"]}"#)
        .send()
        .await
        .expect("mget through Envoy")
        .json()
        .await
        .expect("json");
    let docs = mget["docs"].as_array().expect("docs array");
    assert_eq!(docs.len(), 2, "two fetched docs: {mget}");
    for (doc, want_id) in docs.iter().zip(["1", "10"]) {
        assert!(doc["found"].as_bool().unwrap_or(false), "found: {doc}");
        assert_eq!(doc["_index"], json!("orders"), "logical index: {doc}");
        assert_eq!(doc["_id"], json!(want_id), "logical id: {doc}");
        assert!(
            doc["_source"].get("_tenant").is_none(),
            "isolation field stripped: {doc}"
        );
    }

    // M3b: `_msearch` demux THROUGH Envoy — two searches in one request; each is
    // pinned to the physical index and partition-filtered, and each response's
    // hits come back in the logical view, isolated to acme's 3 docs.
    let msearch_body = "{}\n{\"query\":{\"match_all\":{}},\"size\":20}\n\
                        {}\n{\"query\":{\"term\":{\"who\":\"a10\"}}}\n";
    let msearch: Value = http
        .post(format!("{base}/orders/_msearch"))
        .header("x-tenant", "acme")
        .header("content-type", "application/x-ndjson")
        .body(msearch_body)
        .send()
        .await
        .expect("msearch through Envoy")
        .json()
        .await
        .expect("json");
    let responses = msearch["responses"].as_array().expect("responses array");
    assert_eq!(responses.len(), 2, "two search responses: {msearch}");
    // First (match_all) sees exactly acme's 3 docs, logical ids, no leakage.
    let first_hits = responses[0]["hits"]["hits"].as_array().expect("hits");
    let first_ids: Vec<&str> = first_hits
        .iter()
        .filter_map(|h| h["_id"].as_str())
        .collect();
    assert_eq!(first_hits.len(), 3, "acme's 3 docs in msearch: {msearch}");
    assert!(
        first_ids.contains(&"1") && first_ids.contains(&"10") && first_ids.contains(&"11"),
        "logical ids in msearch response: {first_ids:?}"
    );
    for hit in first_hits {
        assert!(
            hit["_source"].get("_tenant").is_none(),
            "isolation field stripped in msearch hit: {hit}"
        );
    }
    // Second (term who=a10) narrows to the one matching doc, id 10 logical.
    let second_hits = responses[1]["hits"]["hits"].as_array().expect("hits");
    assert_eq!(second_hits.len(), 1, "term search matches one: {msearch}");
    assert_eq!(second_hits[0]["_id"], json!("10"), "logical id in term hit");
}
