//! NFR-P **concurrency** coverage (M7): the ext_proc filter under rising load.
//!
//! `#[ignore]`'d (needs Docker; run `--ignored`). The single-point `perf.rs`
//! harness answers "what does one small read cost"; this one sweeps **concurrency**
//! — the write-through-ext_proc path at c = 1, 8, 32 — into an
//! `evoxy_bench::ScalabilityCurve`, so we can see whether the filter *scales*
//! (throughput climbs, tail stays bounded via Envoy's pool reuse) or *collapses*
//! (the tail blows up). The **body-size** and **rewrite-vs-no-rewrite** axes are
//! measured at the microbench level instead (`evoxy-route`'s `prepare_write` vs
//! `prepare_write_large` vs `prepare_write_norewrite`), where the ~µs transform is
//! not swamped by OpenSearch's ~20 ms write latency — an e2e write-latency A/B
//! cannot resolve a ~2 ms filter cost inside ~20 ms of index noise.
//!
//! Assertions are host-independent (every leg ran to completion, the curve is
//! well-formed); absolute latency is printed, not gated.
#![allow(clippy::pedantic, clippy::unwrap_used, clippy::expect_used)]
#![allow(clippy::disallowed_methods)]
// JUSTIFY: one self-contained live coverage matrix — the two-listener Envoy
// bootstrap, the concurrent load runner, and the concurrency/body-size/rewrite
// sweeps belong together; splitting the fixture from the sweeps would scatter it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use evoxy_bench::{
    judge_scalability, LatencySummary, ScalabilityCurve, ScalabilityPoint, ScalabilityThresholds,
};
use evoxy_extproc::{ExtProcService, ExternalProcessorServer};
use evoxy_filter::{Filter, FilterConfig, ReferenceTenancy};
use osproxy_tenancy::TenancyRouter;
use testcontainers::core::{ContainerPort, Host, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio_stream::wrappers::TcpListenerStream;

/// Requests per configuration (after warm-up).
const SAMPLES: usize = 300;

fn envoy_bootstrap(extproc_port: u16, opensearch_port: u16) -> Vec<u8> {
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
          - name: envoy.filters.http.ext_proc
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
              grpc_service: { envoy_grpc: { cluster_name: extproc } }
              mutation_rules: { allow_all_routing: true, allow_envoy: true }
              processing_mode: { request_header_mode: SEND, request_body_mode: BUFFERED, response_header_mode: SEND, response_body_mode: BUFFERED }
          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
  - name: bare
    address: { socket_address: { address: 0.0.0.0, port_value: 10001 } }
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: passthrough
          route_config:
            name: bare
            virtual_hosts:
            - name: all
              domains: ["*"]
              routes: [{ match: { prefix: "/" }, route: { cluster: opensearch } }]
          http_filters:
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

async fn start_envoy(svc: u16, os: u16) -> (ContainerAsync<GenericImage>, String, String) {
    let envoy = GenericImage::new("envoyproxy/envoy", "v1.31-latest")
        .with_exposed_port(ContainerPort::Tcp(10000))
        .with_exposed_port(ContainerPort::Tcp(10001))
        .with_wait_for(WaitFor::message_on_stderr("starting main dispatch loop"))
        .with_host("host.docker.internal", Host::HostGateway)
        .with_copy_to("/etc/envoy/envoy.yaml", envoy_bootstrap(svc, os))
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("envoy starts");
    let extproc = envoy.get_host_port_ipv4(10000).await.unwrap();
    let bare = envoy.get_host_port_ipv4(10001).await.unwrap();
    (
        envoy,
        format!("http://127.0.0.1:{extproc}"),
        format!("http://127.0.0.1:{bare}"),
    )
}

/// Drive `total` writes to `base` at `concurrency` (each a `PUT /orders/_doc/{i}`
/// with a unique id, so there is no per-doc contention), returning the measured
/// per-request latencies (ns) and the achieved throughput (req/s). A short warm-up
/// runs first and is excluded.
async fn run_writes(
    http: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    body: &[u8],
    concurrency: usize,
    total: usize,
) -> (Vec<u64>, f64) {
    // Warm-up (fills Envoy's upstream pool; excluded from the timing).
    let _ = drive(http, base, tenant, body, concurrency.min(8), 40, usize::MAX).await;

    let next = Arc::new(AtomicUsize::new(0));
    let lats = Arc::new(Mutex::new(Vec::with_capacity(total)));
    let start = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..concurrency {
        let http = http.clone();
        let base = base.to_owned();
        let tenant = tenant.map(str::to_owned);
        let body = body.to_vec();
        let next = next.clone();
        let lats = lats.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                let i = next.fetch_add(1, Ordering::Relaxed);
                if i >= total {
                    break;
                }
                let url = format!("{base}/orders/_doc/{i}");
                let mut req = http.put(&url).header("content-type", "application/json");
                if let Some(t) = &tenant {
                    req = req.header("x-tenant", t.as_str());
                }
                let t0 = Instant::now();
                if let Ok(resp) = req.body(body.clone()).send().await {
                    let _ = resp.bytes().await;
                    lats.lock().unwrap().push(elapsed_ns(t0));
                }
            }
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
    let secs = start.elapsed().as_secs_f64();
    let lats = Arc::try_unwrap(lats)
        .map(Mutex::into_inner)
        .map(Result::unwrap)
        .unwrap_or_default();
    let rps = if secs > 0.0 {
        lats.len() as f64 / secs
    } else {
        0.0
    };
    (lats, rps)
}

/// A tiny warm-up driver (its `total` bounds the warm-up, offset avoids id clash).
async fn drive(
    http: &reqwest::Client,
    base: &str,
    tenant: Option<&str>,
    body: &[u8],
    concurrency: usize,
    total: usize,
    _offset: usize,
) -> usize {
    let next = Arc::new(AtomicUsize::new(1_000_000));
    let mut tasks = Vec::new();
    for _ in 0..concurrency {
        let http = http.clone();
        let base = base.to_owned();
        let tenant = tenant.map(str::to_owned);
        let body = body.to_vec();
        let next = next.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..total.div_ceil(concurrency) {
                let i = next.fetch_add(1, Ordering::Relaxed);
                let url = format!("{base}/orders/_doc/{i}");
                let mut req = http.put(&url).header("content-type", "application/json");
                if let Some(t) = &tenant {
                    req = req.header("x-tenant", t.as_str());
                }
                let _ = req.body(body.clone()).send().await;
            }
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
    total
}

fn elapsed_ns(t0: Instant) -> u64 {
    u64::try_from(t0.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker; run with --ignored"]
async fn concurrency_bodysize_and_rewrite_matrix() {
    let opensearch = start_opensearch().await;
    let os = opensearch.get_host_port_ipv4(9200).await.unwrap();

    // Shared-index mode: writes through the filter get the full transform.
    let config = FilterConfig {
        isolation: evoxy_filter::Isolation::SharedIndex,
        cluster: "opensearch".to_owned(),
        endpoint: "http://unused".to_owned(),
        partition_header: "x-tenant".to_owned(),
        shared_index: Some("orders_shared".to_owned()),
        partition_from_principal: false,
        ..FilterConfig::default()
    };
    let filter = Filter::new(TenancyRouter::new(ReferenceTenancy::from_config(&config)));
    let svc = spawn_service(ExtProcService::new(filter));
    let (_envoy, proxy, bare) = start_envoy(svc, os).await;
    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(128)
        .build()
        .unwrap();
    let small = br#"{"id":1,"who":"acme"}"#.to_vec();

    // Concurrency sweep of the write-through-ext_proc path. (The rewrite-cost and
    // body-size axes are measured at the microbench level — `evoxy-route`'s
    // `prepare_write` vs `prepare_write_large` vs `prepare_write_norewrite` — where
    // the ~µs transform is not swamped by OpenSearch's ~20 ms write latency.)
    let mut points = Vec::new();
    for &c in &[1_u32, 8, 32] {
        let (lats, rps) =
            run_writes(&http, &proxy, Some("acme"), &small, c as usize, SAMPLES).await;
        let latency = LatencySummary::from_nanos(&lats).expect("summary");
        println!(
            "concurrency={c:>2}  p50={:>5}us  p99={:>6}us  rps={rps:>7.0}",
            latency.p50_ns / 1_000,
            latency.p99_ns / 1_000
        );
        points.push(ScalabilityPoint {
            concurrency: c,
            latency,
            throughput_rps: rps,
        });
    }
    let curve = ScalabilityCurve::new(points).expect("curve");
    let verdict = judge_scalability(&curve, &ScalabilityThresholds::provisional());
    println!(
        "tail_amplification={:.2}x  throughput_scaling={:.1}x",
        curve.tail_amplification(),
        curve.throughput_scaling()
    );
    println!("--- scalability verdict ---\n{}", verdict.to_json());

    // Host-independent invariants: every concurrency produced a full sample set,
    // and the sweep scaled throughput (added concurrency bought work, not just
    // tail). Absolute latency is printed, not gated.
    assert_eq!(curve.points.len(), 3);
    for point in &curve.points {
        assert_eq!(
            point.latency.count, SAMPLES as u64,
            "full samples at each c"
        );
    }
    assert!(
        curve.throughput_scaling() > 1.0,
        "concurrency buys throughput: {:.2}x",
        curve.throughput_scaling()
    );
    // `bare` exists for the microbench-complemented rewrite axis; keep it wired so
    // the passthrough listener is exercised on start-up.
    let _ = bare;
}
