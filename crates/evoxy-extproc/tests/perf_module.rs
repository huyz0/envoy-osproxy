//! NFR-P latency proof for the **dynamic-module** backend — the symmetric twin of
//! `perf.rs` (which measures the ext_proc backend). Together they make the
//! ext-vs-dynamic-module comparison a real, measured head-to-head, not an estimate.
//!
//! `#[ignore]`'d (needs Docker; run `--ignored`). It times the *same* GET-by-id
//! three ways against one real OpenSearch, so the overhead is **attributed**:
//! - **baseline** — client → OpenSearch directly;
//! - **envoy-only** — client → a stock Envoy listener with *no* filter → OpenSearch
//!   (isolates Envoy's own proxying cost);
//! - **module** — client → Envoy + our in-process dynamic module → OpenSearch.
//!
//! Then `Envoy overhead = envoy-only − baseline` and `module overhead = module −
//! envoy-only`. Because the module runs the *same* `evoxy-filter` brain as the
//! ext_proc service (ADR-001/004) but **in-process** (no gRPC hop), the `module −
//! envoy-only` delta measured here vs. the `proxy − envoy-only` delta in `perf.rs`
//! is exactly the transport differentiator docs/12 reasons about.
//!
//! ## Prerequisite: the image
//! This drives a STOCK `envoyproxy/envoy:v1.37.0` with our `.so` baked in — build
//! it first (from `~/work`, the parent of both repos):
//! ```text
//! docker build -f envoy-osproxy/crates/evoxy-module/docker/Dockerfile \
//!              -t evoxy-envoy:v1.37.0 .
//! ```
//! The module is loaded via the upstream `DynamicModuleFilter` (ADR-004) — no fork,
//! no rebuild of Envoy, exactly the "capabilities on stock Envoy" thesis.
// unwrap/expect are fine in this harness; the helpers are not `#[test]` fns.
#![allow(clippy::pedantic, clippy::unwrap_used, clippy::expect_used)]
// A latency benchmark measures real wall-clock time — the injected-Clock
// determinism rule (docs/09) is for library code, not for the thing timing I/O.
#![allow(clippy::disallowed_methods)]
// JUSTIFY: one self-contained live NFR-P harness — the Envoy bootstrap, the
// container setup, and the A/B timing loop belong together, mirroring perf.rs.

use std::time::{Duration, Instant};

use osproxy_bench::{judge, LatencySummary, NfrProfile, NfrThresholds};
use serde_json::Value;
use testcontainers::core::{ContainerPort, Host, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// Requests timed on each side after warm-up.
const SAMPLES: usize = 100;
/// Warm-up requests (JIT, pool fill, page cache) excluded from the summary.
const WARMUP: usize = 20;

/// The image built by `evoxy-module/docker/Dockerfile`: stock Envoy + our `.so`.
const IMAGE: &str = "evoxy-envoy";
const IMAGE_TAG: &str = "v1.37.0";

fn envoy_bootstrap(opensearch_port: u16) -> Vec<u8> {
    // Two listeners: `main` (10000) runs the dynamic module; `bare` (10001) is a
    // router-only passthrough to isolate Envoy's own overhead. The module's
    // `filter_config` is the reference-tenancy JSON (dedicated mode, `x-tenant`).
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
                value: '{"cluster":"opensearch","endpoint":"http://unused","partition_header":"x-tenant"}'
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
"#;
    TEMPLATE
        .replace("OS_PORT", &opensearch_port.to_string())
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

/// Returns the container and both base URLs: (module listener, bare-passthrough).
async fn start_envoy(os_port: u16) -> (ContainerAsync<GenericImage>, String, String) {
    let envoy = GenericImage::new(IMAGE, IMAGE_TAG)
        .with_exposed_port(ContainerPort::Tcp(10000))
        .with_exposed_port(ContainerPort::Tcp(10001))
        .with_wait_for(WaitFor::message_on_stderr("starting main dispatch loop"))
        .with_host("host.docker.internal", Host::HostGateway)
        .with_copy_to("/etc/envoy/envoy.yaml", envoy_bootstrap(os_port))
        .with_startup_timeout(Duration::from_secs(60))
        .start()
        .await
        .expect("evoxy-envoy starts (build the image first — see the module doc)");
    let module = envoy.get_host_port_ipv4(10000).await.unwrap();
    let bare = envoy.get_host_port_ipv4(10001).await.unwrap();
    (
        envoy,
        format!("http://127.0.0.1:{module}"),
        format!("http://127.0.0.1:{bare}"),
    )
}

/// Time a `GET url` with the given headers `count` times; return per-request
/// nanoseconds. Every measured response must be `200`, or the run is not comparable.
async fn time_gets(
    http: &reqwest::Client,
    url: &str,
    tenant: Option<&str>,
    warmup: usize,
    count: usize,
) -> Vec<u64> {
    let mut samples = Vec::with_capacity(count);
    for i in 0..(warmup + count) {
        let mut req = http.get(url);
        if let Some(t) = tenant {
            req = req.header("x-tenant", t);
        }
        let measured = i >= warmup;
        let start = Instant::now();
        let sent = req.send().await;
        if sent.is_err() && !measured {
            continue;
        }
        let resp = sent.expect("request");
        let status = resp.status();
        let _ = resp.bytes().await;
        let elapsed = start.elapsed();
        if measured {
            assert!(status.is_success(), "GET {url}: {status}");
            samples.push(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));
        }
    }
    samples
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker + the evoxy-envoy image; run with --ignored"]
async fn module_added_latency_profile_vs_direct() {
    let opensearch = start_opensearch().await;
    let os_port = opensearch.get_host_port_ipv4(9200).await.unwrap();

    let (_envoy, base, bare) = start_envoy(os_port).await;
    let http = reqwest::Client::new();

    // Seed one document directly (dedicated mode reads the same physical doc).
    let direct_doc = format!("http://127.0.0.1:{os_port}/orders/_doc/1?refresh=true");
    let seeded = http
        .put(&direct_doc)
        .header("content-type", "application/json")
        .body(r#"{"k":1,"who":"perf"}"#)
        .send()
        .await
        .expect("seed");
    assert!(seeded.status().is_success(), "seed: {}", seeded.status());

    let direct_get = format!("http://127.0.0.1:{os_port}/orders/_doc/1");
    let bare_get = format!("{bare}/orders/_doc/1");
    let module_get = format!("{base}/orders/_doc/1");

    // Three legs to *decompose* the overhead (mirrors perf.rs):
    //   baseline    — client → OpenSearch directly
    //   envoy-only  — client → Envoy (no filter) → OpenSearch
    //   module      — client → Envoy + our in-process dynamic module → OpenSearch
    let baseline_ns = time_gets(&http, &direct_get, None, WARMUP, SAMPLES).await;
    let envoy_ns = time_gets(&http, &bare_get, None, WARMUP, SAMPLES).await;
    let module_ns = time_gets(&http, &module_get, Some("acme"), WARMUP, SAMPLES).await;

    let baseline = LatencySummary::from_nanos(&baseline_ns).expect("baseline summary");
    let envoy_only = LatencySummary::from_nanos(&envoy_ns).expect("envoy summary");
    let module = LatencySummary::from_nanos(&module_ns).expect("module summary");
    let total_module: u64 = module_ns.iter().sum();
    let throughput_rps = if total_module == 0 {
        0.0
    } else {
        SAMPLES as f64 / (total_module as f64 / 1e9)
    };

    let profile = NfrProfile {
        samples: SAMPLES as u64,
        concurrency: 1,
        baseline,
        proxy: module,
        pool_reuse_rate: 1.0,
        throughput_rps,
    };
    let verdict = judge(&profile, &NfrThresholds::provisional());

    println!(
        "--- nfr-profile (dynamic module) ---\n{}",
        profile.to_json()
    );
    println!("--- verdict ---\n{}", verdict.to_json());
    println!(
        "added p50 = {} us, added p99 = {} us",
        profile.added_p50_ns() / 1_000,
        profile.added_p99_ns() / 1_000
    );

    // The overhead decomposition (p50, microseconds):
    //   Envoy overhead  = envoy-only − baseline   (Envoy just being a proxy)
    //   module overhead = module − envoy-only     (our in-process filter's cost)
    let envoy_added = envoy_only.p50_ns.saturating_sub(baseline.p50_ns) / 1_000;
    let module_added = module.p50_ns.saturating_sub(envoy_only.p50_ns) / 1_000;
    println!(
        "--- overhead breakdown (p50, us) ---\n\
         baseline={}  envoy-only={} (+{} Envoy)  module={} (+{} module over Envoy)",
        baseline.p50_ns / 1_000,
        envoy_only.p50_ns / 1_000,
        envoy_added,
        module.p50_ns / 1_000,
        module_added,
    );

    // Host-independent invariants: all three legs ran to completion and were
    // functional; absolute latency is a per-host calibration (printed above).
    assert_eq!(profile.baseline.count, SAMPLES as u64);
    assert_eq!(envoy_only.count, SAMPLES as u64);
    assert_eq!(profile.proxy.count, SAMPLES as u64);
    let via_module: Value = http
        .get(&module_get)
        .header("x-tenant", "acme")
        .send()
        .await
        .expect("module get")
        .json()
        .await
        .expect("json");
    assert_eq!(via_module["_source"]["who"], Value::from("perf"));
}
