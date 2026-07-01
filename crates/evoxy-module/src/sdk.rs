//! The real Envoy dynamic-module ABI binding (feature `sdk`, ADR-004).
//!
//! This is the one host-gated seam: it depends on the Envoy dynamic-modules SDK
//! (which needs libclang via bindgen). It adapts the SDK's synchronous filter
//! callbacks to the pure [`Module`] driver — buffer the request, run the brain on
//! the runtime, and apply the effects to the Envoy handle.
//!
//! ## SDK 0.1.x constraints (documented, not worked around here)
//! - The request headers map is only reachable in the `request_headers` callback
//!   and cannot be enumerated (only `get`/`set` by key). We fetch the specific
//!   headers the pipeline needs and, for now, apply only **body** mutation and the
//!   fail-closed **local reply** at the body phase — the effects the ABI allows
//!   once the full body is buffered. Routing/header rewrites (multi-cluster,
//!   physical-index remap) need the header-phase split (M2), exactly like the
//!   ext_proc backend; the reference-tenancy default artifact routes statically
//!   and needs neither.

use std::sync::Arc;

use envoy_dynamic_modules_rust_sdk::{
    EnvoyFilterInstance, HttpFilter, HttpFilterInstance, RequestBodyBuffer, RequestBodyStatus,
    RequestHeaders, RequestHeadersStatus,
};
use evoxy_abi::{FilterRequest, HttpVersion, MtlsIdentity};
use evoxy_filter::{EnvoyActions, FilterConfig, ReferenceTenancy};
use osproxy_tenancy::TenancyRouter;
use tokio::runtime::Runtime;

use crate::{default_module, Module};

// Register the module entry point (ADR-003: a user artifact swaps `new_http_filter`
// for its own tenancy factory; here it is the reference tenancy).
envoy_dynamic_modules_rust_sdk::init!(new_http_filter);

/// Build the module for a filter-chain config. Called once per configuration.
fn new_http_filter(config: &str) -> Box<dyn HttpFilter> {
    let partition_header = FilterConfig::from_json(config).partition_header;
    // A dedicated runtime drives the async pipeline from Envoy's sync callbacks.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("tokio runtime for the dynamic module");
    let module = default_module(config, runtime.handle().clone());
    Box::new(EvoxyHttpFilter {
        shared: Arc::new(Shared { _runtime: runtime, module, partition_header }),
    })
}

/// Per-configuration state, shared across request instances.
struct Shared {
    // Kept alive for the module's lifetime; the module holds its `Handle`.
    _runtime: Runtime,
    module: Module<TenancyRouter<ReferenceTenancy>>,
    partition_header: String,
}

/// The module filter (one per filter-chain config).
struct EvoxyHttpFilter {
    shared: Arc<Shared>,
}

impl HttpFilter for EvoxyHttpFilter {
    fn new_instance(&mut self, envoy: EnvoyFilterInstance) -> Box<dyn HttpFilterInstance> {
        Box::new(EvoxyInstance { envoy, shared: self.shared.clone(), headers: Vec::new() })
    }
}

/// Per-request instance: buffers the headers we need, then processes at the body
/// phase (a body-less request is processed at the header phase).
struct EvoxyInstance {
    envoy: EnvoyFilterInstance,
    shared: Arc<Shared>,
    headers: Vec<(String, String)>,
}

impl HttpFilterInstance for EvoxyInstance {
    fn request_headers(
        &mut self,
        request_headers: &RequestHeaders,
        end_of_stream: bool,
    ) -> RequestHeadersStatus {
        self.headers = capture_headers(request_headers, &self.shared.partition_header);
        if end_of_stream {
            // No body (e.g. a read): process now.
            self.process(Vec::new());
            RequestHeadersStatus::Continue
        } else {
            // Hold headers; the body-dependent transform happens once buffered.
            RequestHeadersStatus::StopIteration
        }
    }

    fn request_body(
        &mut self,
        _frame: &RequestBodyBuffer,
        end_of_stream: bool,
    ) -> RequestBodyStatus {
        if !end_of_stream {
            return RequestBodyStatus::StopIterationAndBuffer;
        }
        let body = self.envoy.get_request_body_buffer().copy();
        self.process(body);
        RequestBodyStatus::Continue
    }
}

impl EvoxyInstance {
    /// Build the request, run the brain, and apply the recorded effects.
    fn process(&self, body: Vec<u8>) {
        let req = build_request(&self.headers, body.clone());
        let mut actions = SdkActions::new(body);
        let _decision = self.shared.module.on_request(&req, &mut actions);
        actions.apply(&self.envoy);
    }
}

/// Fetch the headers the pipeline reads (the SDK cannot enumerate the map).
fn capture_headers(headers: &RequestHeaders, partition_header: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut want = |key: &str| {
        if let Some(value) = headers.get(key.as_bytes()) {
            out.push((key.to_owned(), String::from_utf8_lossy(value).into_owned()));
        }
    };
    for key in [":method", ":path", ":authority", "content-type", "x-request-id"] {
        want(key);
    }
    want(partition_header);
    out
}

/// Assemble a [`FilterRequest`] from the captured headers and body.
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
    body: Option<Vec<u8>>,
    immediate: Option<(u16, Vec<u8>)>,
}

impl SdkActions {
    fn new(original_body: Vec<u8>) -> Self {
        Self { original_body, ..Default::default() }
    }

    /// Commit: a fail-closed reply wins; otherwise replace the body iff it changed.
    fn apply(self, envoy: &EnvoyFilterInstance) {
        if let Some((status, body)) = self.immediate {
            envoy.send_response(u32::from(status), &[], &body);
        } else if let Some(body) = self.body {
            if body != self.original_body {
                envoy.get_request_body_buffer().replace(&body);
            }
        }
    }
}

impl EnvoyActions for SdkActions {
    // Routing/header rewrites are not applicable at the body phase in this SDK
    // (see the module docs); the reference tenancy static-routes, so these are
    // no-ops for the default artifact. M2 adds the header-phase split.
    fn set_upstream_cluster(&mut self, _cluster: &str) {}
    fn set_method(&mut self, _method: &str) {}
    fn set_path(&mut self, _path: &str) {}
    fn set_header(&mut self, _name: &str, _value: &str) {}
    fn remove_header(&mut self, _name: &str) {}
    fn set_body(&mut self, body: &[u8]) {
        self.body = Some(body.to_vec());
    }
    fn send_local_reply(&mut self, status: u16, _headers: &[(String, String)], body: &[u8]) {
        self.immediate = Some((status, body.to_vec()));
    }
}
