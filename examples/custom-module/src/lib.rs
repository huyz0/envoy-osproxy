//! Example: a **custom-tenancy dynamic module**, in full.
//!
//! This is the entire cdylib. `evoxy_module_sdk::register!` takes a factory that
//! turns Envoy's `filter_config` blob into an `evoxy_filter::Filter`; the macro
//! generates Envoy's module entry point and wires the factory in. The Envoy SDK is
//! pulled in transitively by `evoxy-module-sdk`, so this crate never names it.
//!
//! Build the `.so` (needs clang + libclang):
//!
//! ```sh
//! cargo build --release   # target/release/libcustom_module.so
//! ```
//!
//! Then drop it into a stock Envoy's dynamic-modules search path (the
//! [`crates/evoxy-module/docker/Dockerfile`](../../crates/evoxy-module/docker/Dockerfile)
//! shows how) and point Envoy's `DynamicModuleFilter` at it.

use custom_tenancy::TieredTenancy;
use evoxy_module_sdk::Filter;
use osproxy_tenancy::TenancyRouter;

evoxy_module_sdk::register!(|config: &str| {
    // `config` is Envoy's `filter_config` blob. Parse whatever knobs your tenancy
    // needs from it; this example uses fixed tiers to stay readable.
    let _ = config;
    let tenancy = TieredTenancy {
        partition_header: "x-tenant".to_owned(),
        cluster: "opensearch".to_owned(),
        premium: ["acme".to_owned()].into_iter().collect(),
    };
    // Wrap the tenancy in a Filter. Add filter-level options here if you want them,
    // e.g. `.with_passthrough_indices(["catalog".to_owned()])`.
    Filter::new(TenancyRouter::new(tenancy))
});
