//! The Envoy dynamic-module cdylib (ADR-004).
//!
//! Two layers:
//! - a **driver** ([`Module`]) that is pure Rust over [`evoxy-filter`] — it holds
//!   the [`Filter`] brain and a Tokio runtime handle, and runs one request. This
//!   builds and is reviewable anywhere (no SDK, no libclang).
//! - the **SDK binding** (the `sdk` module, behind `--features sdk`): implements
//!   the Envoy SDK's filter trait, adapts each callback to
//!   [`evoxy_filter::EnvoyActions`], and registers the module. Host-gated.
//!
//! The whole point of the split (ADR-004) is that everything the driver does is
//! exercised by `evoxy-filter`'s tests; the SDK layer is a thin, mechanical
//! adapter with no business logic.

use evoxy_abi::FilterRequest;
use evoxy_filter::{EnvoyActions, Filter, FilterDecision, ReferenceTenancy};
use osproxy_tenancy::{Router, TenancyRouter};
use tokio::runtime::Handle;

/// A loaded module: the request-handling brain plus the runtime handle used to
/// drive its async work from Envoy's synchronous filter callbacks.
pub struct Module<R> {
    filter: Filter<R>,
    runtime: Handle,
}

impl<R: Router> Module<R> {
    /// Build a module over a resolved router and a runtime handle.
    pub fn new(router: R, runtime: Handle) -> Self {
        Self { filter: Filter::new(router), runtime }
    }

    /// Handle one buffered request, driving the async pipeline to completion on
    /// the runtime. Envoy filter callbacks are synchronous, so we `block_on`; the
    /// reference/in-memory placements resolve without I/O, so this does not block
    /// on the network (ADR-004). Returns whether Envoy should continue upstream.
    pub fn on_request(&self, req: &FilterRequest, actions: &mut dyn EnvoyActions) -> FilterDecision {
        self.runtime.block_on(self.filter.handle(req, actions))
    }
}

/// Build the default module (the reference tenancy) from an Envoy `filter_config`
/// JSON blob and a runtime handle. A user artifact replaces this with its own
/// `TenancySpi` via the `register!` factory (ADR-003).
pub fn default_module(
    filter_config: &str,
    runtime: Handle,
) -> Module<TenancyRouter<ReferenceTenancy>> {
    let config = evoxy_filter::FilterConfig::from_json(filter_config);
    let tenancy = ReferenceTenancy::from_config(&config);
    Module::new(TenancyRouter::new(tenancy), runtime)
}

// SDK: the real Envoy ABI binding lives here, behind `--features sdk`, and is the
// only host-gated code (needs libclang + the Envoy SDK). It (1) implements
// `EnvoyActions` over the SDK's request handle — `set_method`/`set_path` via the
// `:method`/`:path` request headers, body drain+append, header set/remove, and
// `send_local_reply` via `send_response`; and (2) implements the SDK's `HttpFilter`/
// `HttpFilterConfig` traits by enumerating the headers and buffering the body into a
// `FilterRequest`, calling `Module::on_request`, and applying the recorded effects;
// and (3) invokes `declare_init_functions!`. Uses the OFFICIAL upstream SDK pinned to
// the Envoy release tag (the ABI hash is load-checked). See README.md.
#[cfg(feature = "sdk")]
mod sdk;
