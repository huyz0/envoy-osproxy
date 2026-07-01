# 06 — Wiring example: how a user implements the SPI

This is the envoy-osproxy mirror of osproxy's wiring guide. The SPI traits are the
**same** (`osproxy-spi`); the difference is you do not write a `main` or a `Sink`
— Envoy is the app and forwards upstream (ADR-002, ADR-003).

> Status: illustrative — the `evoxy-filter` `register!` API lands in M1 (1b). The
> shape below is the contract we are building to.

## What you write

One crate: your tenancy/routing brain, plus a one-line registration.

```toml
# their-tenancy/Cargo.toml
[package]
name = "acme-tenancy"

[lib]
crate-type = ["cdylib"]        # a dynamic module Envoy loads (or a bin for ext_proc)

[dependencies]
evoxy-filter = "…"             # our library: register! + the reused engine
osproxy-spi  = "…"             # the SAME SPI traits as osproxy
```

```rust
// their-tenancy/src/lib.rs
use osproxy_spi::{TenancySpi, RequestCtx, BodyDoc};
use evoxy_filter::{register, FilterConfig};

struct AcmeTenancy { /* your placement policy, tables, … */ }

impl AcmeTenancy {
    fn from_config(cfg: &FilterConfig) -> Self { /* read Envoy filter config + env */ todo!() }
}

impl TenancySpi for AcmeTenancy {
    fn resolve_partition(&self, ctx: &RequestCtx<'_>, body: BodyDoc<'_>) -> /* … */ {
        // your tenancy decision — exactly as in osproxy
    }
    // … the rest of the SPI you need (rules, routing) …
}

// The one seam: hand the filter your configured brain. Called once at Envoy
// module init. No main, no transport, no dispatch.
register!(|cfg: &FilterConfig| AcmeTenancy::from_config(cfg));
```

`cargo build --release` produces `libacme_tenancy.so`. That artifact is **your
SPI + our filter glue + the reused osproxy engine**, statically linked.

## What you do NOT write

- No `main` / process — Envoy is the app.
- No `Sink`/`Reader` / upstream client — Envoy forwards with its own pool (ADR-002).
- No transport/TLS — Envoy terminates.

## How you deploy

Stock Envoy + your `.so` + a bootstrap that (a) loads the module and (b) maps each
logical `ClusterId` your placement returns to an Envoy upstream cluster:

```yaml
# envoy.yaml (sketch)
http_filters:
  - name: envoy.filters.http.dynamic_modules
    typed_config:
      dynamic_module_config: { name: acme_tenancy }   # loads libacme_tenancy.so
      filter_config: { … your config blob … }          # arrives as FilterConfig
clusters:
  - name: eu-1        # <- a logical ClusterId your placement returns
    load_assignment: { … OpenSearch endpoints … }
```

To change tenancy logic: rebuild the `.so`, restart Envoy. Static, not a runtime
plugin (ADR-003 / ADR-007).

## Just trying it out?

Use the **default artifact** we ship, which links a reference tenancy — a runnable
`.so` with no code to write, the mirror of osproxy's `ReferenceTenancy`. Point it
at OpenSearch via the bootstrap and go.
