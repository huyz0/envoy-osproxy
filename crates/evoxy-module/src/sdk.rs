//! The real Envoy dynamic-module ABI binding (feature `sdk`, ADR-004).
//!
//! This is the one host-gated seam: it depends on the OFFICIAL upstream Envoy
//! dynamic-modules SDK (`envoy-proxy-dynamic-modules-rust-sdk`, pinned to the
//! Envoy release tag — the ABI hash is checked at load, so SDK tag ⇔ image tag).
//! It adapts the SDK's synchronous filter callbacks to the pure [`Module`] driver
//! — buffer the request, run the brain on the runtime, and apply the effects to
//! the Envoy handle.
//!
//! Unlike the earlier prototype ABI, this SDK can **enumerate** the request header
//! map and **mutate** headers (`:method`/`:path` included) plus the body buffer, so
//! the module applies the *full* transform-then-forward (ADR-002): path rewrite,
//! header inject, body splice, and the fail-closed local reply. Cluster override
//! is not exposed by this SDK rev; the reference tenancy static-routes to the one
//! configured upstream, so `set_upstream_cluster` is recorded but not applied here.

use std::sync::Arc;

use envoy_proxy_dynamic_modules_rust_sdk::{
    abi, declare_init_functions, EnvoyHttpFilter, EnvoyHttpFilterConfig, HttpFilter,
    HttpFilterConfig,
};
use evoxy_abi::{FilterRequest, HttpVersion, MtlsIdentity};
use evoxy_filter::{EnvoyActions, FilterConfig, ReferenceTenancy};
use osproxy_tenancy::TenancyRouter;
use tokio::runtime::Runtime;

use crate::{default_module, Module};

// Register the module entry points. `envoy_dynamic_module_on_program_init` returns
// the SDK's `kAbiVersion`, which Envoy verifies against its own compiled-in hash.
declare_init_functions!(init, new_http_filter_config_fn);

/// Called once when Envoy loads the module. Returning `false` rejects the config.
fn init() -> bool {
    true
}

/// Build the per-filter-chain config. `filter_name` selects the behaviour (we ship
/// one, `evoxy`); `filter_config` is the JSON blob from the Envoy config.
fn new_http_filter_config_fn<EC: EnvoyHttpFilterConfig, EHF: EnvoyHttpFilter>(
    _envoy_filter_config: &mut EC,
    filter_name: &str,
    filter_config: &[u8],
) -> Option<Box<dyn HttpFilterConfig<EHF>>> {
    let config = String::from_utf8_lossy(filter_config).into_owned();
    match filter_name {
        // The reference-tenancy artifact. A user artifact swaps this for its own
        // `TenancySpi` factory (ADR-003).
        "evoxy" | "" => Some(Box::new(EvoxyConfig::new(&config))),
        // Fail closed on an unknown filter name (INV-3: no surprises), but do not
        // panic across the C ABI into the Envoy worker — reject the config.
        _ => None,
    }
}

/// Per-filter-chain state, shared across request instances. Holds the runtime that
/// drives the async pipeline from Envoy's synchronous callbacks, and the brain.
struct Shared {
    // Kept alive for the config's lifetime; the module holds its `Handle`.
    _runtime: Runtime,
    module: Module<TenancyRouter<ReferenceTenancy>>,
    #[allow(dead_code)] // reserved for header-targeted tenancy variants (ADR-003).
    partition_header: String,
}

/// The SDK filter-config object (one per filter-chain configuration).
struct EvoxyConfig {
    shared: Arc<Shared>,
}

impl EvoxyConfig {
    fn new(config: &str) -> Self {
        let partition_header = FilterConfig::from_json(config).partition_header;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("tokio runtime for the dynamic module");
        let module = default_module(config, runtime.handle().clone());
        Self { shared: Arc::new(Shared { _runtime: runtime, module, partition_header }) }
    }
}

impl<EHF: EnvoyHttpFilter> HttpFilterConfig<EHF> for EvoxyConfig {
    fn new_http_filter(&self, _envoy: &mut EHF) -> Box<dyn HttpFilter<EHF>> {
        Box::new(EvoxyFilter { shared: self.shared.clone(), headers: Vec::new() })
    }
}

/// Per-request instance: captures the headers at the header phase, then runs the
/// brain — at the header phase for a body-less request (a read), otherwise once the
/// body is fully buffered.
struct EvoxyFilter {
    shared: Arc<Shared>,
    headers: Vec<(String, String)>,
}

impl<EHF: EnvoyHttpFilter> HttpFilter<EHF> for EvoxyFilter {
    fn on_request_headers(
        &mut self,
        envoy: &mut EHF,
        end_of_stream: bool,
    ) -> abi::envoy_dynamic_module_type_on_http_filter_request_headers_status {
        self.headers = envoy
            .get_request_headers()
            .into_iter()
            .map(|(k, v)| {
                (
                    String::from_utf8_lossy(k.as_slice()).into_owned(),
                    String::from_utf8_lossy(v.as_slice()).into_owned(),
                )
            })
            .collect();
        if end_of_stream {
            // No body (a read): run the brain now.
            self.process(envoy, Vec::new());
            abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::Continue
        } else {
            // Hold headers; the body-dependent transform runs once buffered.
            abi::envoy_dynamic_module_type_on_http_filter_request_headers_status::StopIteration
        }
    }

    fn on_request_body(
        &mut self,
        envoy: &mut EHF,
        end_of_stream: bool,
    ) -> abi::envoy_dynamic_module_type_on_http_filter_request_body_status {
        if !end_of_stream {
            return abi::envoy_dynamic_module_type_on_http_filter_request_body_status::StopIterationAndBuffer;
        }
        let body = envoy
            .get_buffered_request_body()
            .map(|slices| slices.iter().flat_map(|s| s.as_slice().to_vec()).collect())
            .unwrap_or_default();
        self.process(envoy, body);
        abi::envoy_dynamic_module_type_on_http_filter_request_body_status::Continue
    }
}

impl EvoxyFilter {
    /// Build the request, run the brain, and apply the recorded effects.
    fn process<EHF: EnvoyHttpFilter>(&self, envoy: &mut EHF, body: Vec<u8>) {
        let req = build_request(&self.headers, body.clone());
        let mut actions = SdkActions::new(body);
        let _decision = self.shared.module.on_request(&req, &mut actions);
        actions.apply(envoy);
    }
}

/// Assemble a [`FilterRequest`] from the captured headers and body. The pseudo
/// headers (`:method`/`:path`/`:authority`) come from the enumerated header map.
fn build_request(headers: &[(String, String)], body: Vec<u8>) -> FilterRequest {
    let get = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    };
    FilterRequest {
        method: get(":method").unwrap_or_default(),
        path_and_query: get(":path").unwrap_or_default(),
        authority: get(":authority").unwrap_or_default(),
        version: HttpVersion::Http2,
        headers: headers.to_vec(),
        body,
        identity: MtlsIdentity::default(),
    }
}

/// An owned recorder of the brain's effects (so it stays `Send`, as
/// [`EnvoyActions`] requires); [`SdkActions::apply`] commits them to the Envoy
/// handle after the async pipeline completes.
#[derive(Default)]
struct SdkActions {
    original_body: Vec<u8>,
    method: Option<String>,
    path: Option<String>,
    body: Option<Vec<u8>>,
    set_headers: Vec<(String, String)>,
    remove_headers: Vec<String>,
    immediate: Option<(u16, Vec<u8>)>,
}

impl SdkActions {
    fn new(original_body: Vec<u8>) -> Self {
        Self { original_body, ..Default::default() }
    }

    /// Commit: a fail-closed reply wins (stop the chain); otherwise apply the
    /// header/method/path rewrites and replace the body iff it changed.
    fn apply<EHF: EnvoyHttpFilter>(self, envoy: &mut EHF) {
        if let Some((status, body)) = self.immediate {
            envoy.send_response(u32::from(status), Vec::new(), Some(&body), None);
            return;
        }
        if let Some(method) = self.method {
            envoy.set_request_header(":method", method.as_bytes());
        }
        if let Some(path) = self.path {
            envoy.set_request_header(":path", path.as_bytes());
        }
        for (name, value) in self.set_headers {
            envoy.set_request_header(&name, value.as_bytes());
        }
        for name in self.remove_headers {
            envoy.remove_request_header(&name);
        }
        if let Some(body) = self.body {
            if body != self.original_body {
                // Replace the buffered body: drain the old bytes, append the new.
                let existing = envoy.get_buffered_request_body_size();
                if existing > 0 {
                    envoy.drain_buffered_request_body(existing);
                }
                envoy.append_buffered_request_body(&body);
            }
        }
    }
}

impl EnvoyActions for SdkActions {
    // Cluster override is not exposed by this SDK rev; the reference tenancy
    // static-routes to the one configured upstream, so this is recorded-then-ignored
    // (the physical-index rewrite rides on `set_path`).
    fn set_upstream_cluster(&mut self, _cluster: &str) {}
    fn set_method(&mut self, method: &str) {
        self.method = Some(method.to_owned());
    }
    fn set_path(&mut self, path: &str) {
        self.path = Some(path.to_owned());
    }
    fn set_body(&mut self, body: &[u8]) {
        self.body = Some(body.to_vec());
    }
    fn set_header(&mut self, name: &str, value: &str) {
        self.set_headers.push((name.to_owned(), value.to_owned()));
    }
    fn remove_header(&mut self, name: &str) {
        self.remove_headers.push(name.to_owned());
    }
    fn send_local_reply(&mut self, status: u16, _headers: &[(String, String)], body: &[u8]) {
        self.immediate = Some((status, body.to_vec()));
    }
}
