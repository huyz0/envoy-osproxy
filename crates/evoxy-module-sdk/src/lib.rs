//! Build an Envoy dynamic module over the evoxy brain (ADR-004).
//!
//! This is the reusable library a dynamic-module cdylib depends on. Two layers:
//! - a **driver** ([`Module`]) that is pure Rust over [`evoxy-filter`] — it holds
//!   the [`Filter`] brain and a Tokio runtime handle, and runs one request.
//! - the **SDK binding** ([`sdk`]): implements the Envoy SDK's filter traits over
//!   [`evoxy_filter::EnvoyActions`], generic over any tenancy. It links the
//!   upstream Envoy dynamic-modules SDK (git-pinned to the Envoy release tag, its
//!   ABI hash checked at load), so this crate needs `libclang` and is not on
//!   crates.io — but *your* cdylib never publishes, so a git dependency on it is
//!   fine (crates.io only forbids git deps at publish time).
//!
//! # Building your own module
//!
//! Your whole cdylib is one [`register!`] call with a factory that turns Envoy's
//! `filter_config` blob into a [`Filter`]:
//!
//! ```ignore
//! evoxy_module_sdk::register!(|config: &str| {
//!     let tenancy = my_tenancy::MyTenancy::from_json(config);
//!     evoxy_filter::Filter::new(osproxy_tenancy::TenancyRouter::new(tenancy))
//! });
//! ```
//!
//! Depend on this crate, invoke the macro once at your crate root, and build the
//! `cdylib`. The Envoy SDK is pulled in transitively — you never name it. See
//! `examples/custom-module`.

use std::sync::Arc;

use evoxy_abi::FilterRequest;
use evoxy_filter::{
    wants_async, EnvoyActions, FilterDecision, ImmediateReply, Observe, ReferenceTenancy,
};
use osproxy_tenancy::{Router, TenancyRouter};
use tokio::runtime::Handle;

pub mod sdk;

// Re-exported so a cdylib can build its `Filter` without a separate `evoxy-filter`
// dependency: `evoxy_module_sdk::Filter::new(router)`. `FilterConfig` lets a custom
// module reuse the reference config parser; `ObserveConfig` parses the reserved
// observability keys (`admin_token`, `emit_decision`) from the same blob;
// `AsyncWriteSink` is the async-write fan-out seam a `register_async!` module wires.
pub use evoxy_filter::{Filter, FilterConfig, ObserveConfig};
pub use evoxy_filter::{AsyncWriteSink, WRITE_MODE_HEADER};

/// A loaded module: the request-handling brain, the shared observe surface (the
/// reserved introspection paths and the directive plane), an optional async-write
/// sink, and the runtime handle used to drive their async work from Envoy's
/// synchronous filter callbacks.
pub struct Module<R> {
    filter: Filter<R>,
    observe: Observe,
    async_sink: Option<Arc<dyn AsyncWriteSink>>,
    runtime: Handle,
}

impl<R: Router> Module<R> {
    /// Build a module over a configured [`Filter`] and a runtime handle, with a
    /// default observe surface (metrics and explain on, directive plane fail-closed)
    /// and async write mode off. Use [`with_observe`](Self::with_observe) /
    /// [`with_async_sink`](Self::with_async_sink) to enable those.
    pub fn new(filter: Filter<R>, runtime: Handle) -> Self {
        Self {
            filter,
            observe: Observe::default(),
            async_sink: None,
            runtime,
        }
    }

    /// Set the observe surface (from the `filter_config` reserved keys, so the
    /// directive plane is enabled config-only, like the tenancy).
    #[must_use]
    pub fn with_observe(mut self, observe: Observe) -> Self {
        self.observe = observe;
        self
    }

    /// Enable async write mode (ADR-010) by wiring a durable fan-out sink. A write
    /// carrying `x-evoxy-write-mode: async` is then produced to the sink and answered
    /// `202` instead of forwarding.
    ///
    /// Off by default, and deliberately so: awaiting the broker ack **blocks the
    /// Envoy worker thread** the filter runs on (unlike the ext_proc sidecar, which
    /// blocks only its own task), so enable it only when that trade is acceptable.
    #[must_use]
    pub fn with_async_sink(mut self, sink: Arc<dyn AsyncWriteSink>) -> Self {
        self.async_sink = Some(sink);
        self
    }

    /// If the request opts into async write mode, run the shared async-write contract
    /// on the runtime and return the client reply; else `None`, so the caller
    /// forwards normally. Blocks on the broker ack (see
    /// [`with_async_sink`](Self::with_async_sink)).
    pub fn async_write(&self, req: &FilterRequest) -> Option<ImmediateReply> {
        if !wants_async(&req.headers) {
            return None;
        }
        Some(
            self.runtime
                .block_on(self.filter.async_write(req, self.async_sink.as_deref())),
        )
    }

    /// The reserved introspection reply for this request (`/_evoxy/metrics`,
    /// `/_evoxy/explain/...`, `/_evoxy/admin/directives`), or `None` for a normal
    /// data-plane request. The binding renders it via `send_response`.
    pub fn reserved_reply(&self, headers: &[(String, String)]) -> Option<ImmediateReply> {
        self.runtime
            .block_on(self.observe.reserved_reply(&self.filter, headers))
    }

    /// The shape-only `x-evoxy-decision` header value for a response, or `None` when
    /// the directive plane has silenced it.
    pub fn decision_header(&self, headers: &[(String, String)]) -> Option<String> {
        self.runtime
            .block_on(self.observe.decision_header(&self.filter, headers))
    }

    /// Record a request forwarded upstream, for `/_evoxy/metrics`.
    pub fn record_routed(&self) {
        self.observe.record_routed();
    }

    /// Record a request answered with a fail-closed immediate reply.
    pub fn record_rejected(&self) {
        self.observe.record_rejected();
    }

    /// Handle one buffered request, driving the async pipeline to completion on
    /// the runtime. Envoy filter callbacks are synchronous, so we `block_on`; the
    /// reference/in-memory placements resolve without I/O, so this does not block
    /// on the network (ADR-004). Returns whether Envoy should continue upstream.
    pub fn on_request(&self, req: &FilterRequest, actions: &mut dyn EnvoyActions) -> FilterDecision {
        self.runtime.block_on(self.filter.handle(req, actions))
    }

    /// Resolve **only** the upstream cluster from the request headers and set it,
    /// at the header phase — before Envoy selects a route. A write buffers its body
    /// before the transform runs, but its cluster is known from the headers, so
    /// naming it here (via `x-evoxy-cluster`) lets Envoy route on it; the body-phase
    /// [`on_request`](Self::on_request) then applies the path/body transform. Reads
    /// resolve fully at the header phase and do not need this. Returns whether to
    /// continue (a resolution error sends a fail-closed reply and stops).
    pub fn route_headers(
        &self,
        req: &FilterRequest,
        actions: &mut dyn EnvoyActions,
    ) -> FilterDecision {
        self.runtime.block_on(self.filter.route_headers(req, actions))
    }

    /// Reshape a read's upstream response into the client's logical view (strip
    /// injected fields, map physical ids back to logical). `req` is rebuilt from the
    /// captured request headers; `upstream_body` is the cluster's response body.
    /// Returns the shaped body, or `None` when there is nothing to do (the caller
    /// then forwards the upstream body unchanged). Resolves without I/O, like
    /// [`Module::on_request`].
    pub fn on_response(&self, req: &FilterRequest, upstream_body: &[u8]) -> Option<Vec<u8>> {
        self.runtime
            .block_on(self.filter.shape_response(req, upstream_body))
    }
}

/// The reference-tenancy [`Filter`], built from an Envoy `filter_config` blob. This
/// is the factory the default `evoxy-module` artifact registers; a user artifact
/// passes its own factory to [`register!`] instead (ADR-003).
#[must_use]
pub fn reference_filter(filter_config: &str) -> Filter<TenancyRouter<ReferenceTenancy>> {
    evoxy_filter::reference_filter(&evoxy_filter::FilterConfig::from_json(filter_config))
}

/// Register a dynamic module over your tenancy. Give it a factory
/// `fn(&str) -> Filter<_>` (a non-capturing closure or a `fn` path works) that turns
/// Envoy's `filter_config` blob into a [`Filter`]; the macro generates Envoy's
/// `on_program_init` entry point and wires your factory in.
///
/// Invoke it **once** at the crate root of your `cdylib`. The upstream Envoy SDK is
/// pulled in transitively — your crate never names it. See `examples/custom-module`.
#[macro_export]
macro_rules! register {
    ($factory:expr) => {
        /// Envoy's module init entry point: installs the config factory and returns
        /// the SDK ABI-version string Envoy verifies at load. Generated by
        /// `evoxy_module_sdk::register!`.
        #[no_mangle]
        pub extern "C" fn envoy_dynamic_module_on_program_init() -> *const ::std::os::raw::c_char {
            fn __evoxy_new_config(
                _config: &mut $crate::sdk::ConfigHandle,
                _filter_name: &str,
                filter_config: &[u8],
            ) -> ::core::option::Option<
                ::std::boxed::Box<dyn $crate::sdk::SdkHttpFilterConfig<$crate::sdk::FilterHandle>>,
            > {
                $crate::sdk::new_config(filter_config, $factory)
            }
            $crate::sdk::install(__evoxy_new_config)
        }
    };
}

/// Like [`register!`], but also wires an **async write sink**, enabling async write
/// mode (ADR-010) on the module. Give it two factories: the filter factory
/// (`fn(&str) -> Filter<_>`, as for [`register!`]) and a sink factory
/// (`fn(&str) -> Option<Arc<dyn AsyncWriteSink>>`) that builds a durable fan-out sink
/// (a `Bridge` over an `AckProducer`) from Envoy's `filter_config` blob, or `None` to
/// leave async off.
///
/// A write carrying `x-evoxy-write-mode: async` is then produced to the sink and
/// answered `202`. Awaiting the broker ack **blocks the Envoy worker thread** the
/// filter runs on, so enable this only when that trade is acceptable (see
/// [`Module::with_async_sink`]).
#[macro_export]
macro_rules! register_async {
    ($filter_factory:expr, $sink_factory:expr) => {
        /// Envoy's module init entry point (async-write variant). Generated by
        /// `evoxy_module_sdk::register_async!`.
        #[no_mangle]
        pub extern "C" fn envoy_dynamic_module_on_program_init() -> *const ::std::os::raw::c_char {
            fn __evoxy_new_config(
                _config: &mut $crate::sdk::ConfigHandle,
                _filter_name: &str,
                filter_config: &[u8],
            ) -> ::core::option::Option<
                ::std::boxed::Box<dyn $crate::sdk::SdkHttpFilterConfig<$crate::sdk::FilterHandle>>,
            > {
                $crate::sdk::new_config_async(filter_config, $filter_factory, $sink_factory)
            }
            $crate::sdk::install(__evoxy_new_config)
        }
    };
}
