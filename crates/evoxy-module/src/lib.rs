//! The **reference** Envoy dynamic-module cdylib (ADR-004).
//!
//! This is the "works out of the box" artifact: a stock Envoy loads the built
//! `libevoxy_module.so`, and it runs the reference tenancy. It is one line over
//! [`evoxy-module-sdk`](../evoxy_module_sdk/index.html) — the `register!` macro
//! generates Envoy's module entry point and wires in the reference router (which
//! reads its placement from Envoy's `filter_config` blob).
//!
//! A real deployment builds *its own* cdylib exactly like this, but with its own
//! tenancy factory instead of [`reference_router`](evoxy_module_sdk::reference_router).
//! See `examples/custom-module` — it is the same one line with a custom router.

evoxy_module_sdk::register!(evoxy_module_sdk::reference_router);
