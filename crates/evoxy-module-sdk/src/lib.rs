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
//! `filter_config` blob into a [`Router`](osproxy_tenancy::Router):
//!
//! ```ignore
//! evoxy_module_sdk::register!(|config: &str| {
//!     let tenancy = my_tenancy::MyTenancy::from_json(config);
//!     osproxy_tenancy::TenancyRouter::new(tenancy)
//! });
//! ```
//!
//! Depend on this crate, invoke the macro once at your crate root, and build the
//! `cdylib`. The Envoy SDK is pulled in transitively — you never name it. See
//! `examples/custom-module`.

use evoxy_abi::FilterRequest;
use evoxy_filter::{EnvoyActions, Filter, FilterDecision, ReferenceTenancy};
use osproxy_tenancy::{Router, TenancyRouter};
use tokio::runtime::Handle;

pub mod sdk;

/// A loaded module: the request-handling brain plus the runtime handle used to
/// drive its async work from Envoy's synchronous filter callbacks.
pub struct Module<R> {
    filter: Filter<R>,
    runtime: Handle,
}

impl<R: Router> Module<R> {
    /// Build a module over a resolved router and a runtime handle.
    pub fn new(router: R, runtime: Handle) -> Self {
        Self {
            filter: Filter::new(router),
            runtime,
        }
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

/// The reference tenancy as a [`Router`], built from an Envoy `filter_config` blob.
/// This is the factory the default `evoxy-module` artifact registers; a user
/// artifact passes its own factory to [`register!`] instead (ADR-003).
#[must_use]
pub fn reference_router(filter_config: &str) -> TenancyRouter<ReferenceTenancy> {
    let config = evoxy_filter::FilterConfig::from_json(filter_config);
    TenancyRouter::new(ReferenceTenancy::from_config(&config))
}

/// Register a dynamic module over your tenancy. Give it a factory
/// `fn(&str) -> impl Router` (a non-capturing closure or a `fn` path works) that
/// turns Envoy's `filter_config` blob into a [`Router`](osproxy_tenancy::Router);
/// the macro generates Envoy's `on_program_init` entry point and wires your factory
/// in.
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
