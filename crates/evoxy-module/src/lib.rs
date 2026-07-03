//! The **reference** Envoy dynamic-module cdylib (ADR-004).
//!
//! This is the "works out of the box" artifact: a stock Envoy loads the built
//! `libevoxy_module.so`, and it runs the reference tenancy. It is one line over
//! [`evoxy-module-sdk`](../evoxy_module_sdk/index.html). The `register!` macro
//! generates Envoy's module entry point and wires in the reference-tenancy filter
//! (which reads its config from Envoy's `filter_config` blob).
//!
//! A real deployment builds *its own* cdylib exactly like this, but with its own
//! factory instead of [`reference_filter`](evoxy_module_sdk::reference_filter). See
//! `examples/custom-module`, the same one line with a custom filter.

evoxy_module_sdk::register!(evoxy_module_sdk::reference_filter);
