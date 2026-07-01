# Build the evoxy dynamic module and bake it into a STOCK Envoy image (ADR-004).
#
# This does NOT fork or rebuild Envoy — it uses the upstream `envoyproxy/envoy`
# binary unchanged and only drops our `.so` into its dynamic-modules search path.
# The Envoy tag here MUST equal the SDK tag pinned in evoxy-module/Cargo.toml
# (the ABI-header hash is checked at load).
#
# Build context is this repository root (self-contained — the engine crates are
# pulled from crates.io, no sibling checkout). From the repo root:
#   docker build -f crates/evoxy-module/docker/Dockerfile -t evoxy-envoy:v1.37.0 .
# or simply `cargo xtask module-image`.

# Builder on Debian bookworm (glibc 2.36) — older than the Envoy image's Ubuntu
# 24.04 (glibc 2.39), so the produced .so loads there (glibc is forward-compatible).
FROM rust:1.94-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends clang \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .

WORKDIR /src/crates/evoxy-module
# `--features sdk` pulls the upstream SDK (git dep) and binds Envoy's C ABI via
# bindgen (clang). Release profile is `panic = "abort"` (no unwind across the ABI).
RUN cargo build --release --features sdk

# Stock upstream Envoy, unmodified.
FROM envoyproxy/envoy:v1.37.0
ENV ENVOY_DYNAMIC_MODULES_SEARCH_PATH=/usr/local/lib
COPY --from=builder \
    /src/crates/evoxy-module/target/release/libevoxy_module.so \
    /usr/local/lib/libevoxy_module.so
