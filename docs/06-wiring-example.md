# 06. Using envoy-osproxy: implement the SPI, build an artifact, configure Envoy

envoy-osproxy is a **toolkit, not a turnkey proxy**, there is no ready-to-run
binary. A deployment does three things; the runnable, compiling versions live in
[`examples/`](https://github.com/huyz0/envoy-osproxy/tree/main/examples) (this page is the narrative, `examples/README.md` is the
step-by-step).

## What you write

**1. The tenancy SPI.** Your placement/isolation logic is an
`osproxy_spi::TenancySpi`, the *same* trait the standalone osproxy uses (that is
the reuse: same brain, different transport). You implement `resolve_partition`,
`doc_id_rule`, `injected_fields`, and `placement_for`.
[`examples/custom-tenancy`](https://github.com/huyz0/envoy-osproxy/tree/main/examples/custom-tenancy) is a real, compiling
example. You do **not** write a `main`, a `Sink`, an upstream client, or any TLS
Envoy is the app and forwards upstream (ADR-002).

**2. An artifact**, one of:
- a **dynamic-module `.so`** (in-process, lowest latency), `evoxy-module` is
  generic over your router, so you wire your tenancy into its factory and build
  (`cargo xtask module-image`);
- an **ext_proc gRPC server** (out-of-process, isolated), a small `tonic`/`tokio`
  binary serving `evoxy_extproc::ExtProcService`, generic over your tenancy just
  like the module (see `examples/README.md`).

**3. The Envoy bootstrap**, stock Envoy, no rebuild: load the artifact and map
each logical `ClusterId` your placement returns to a real upstream cluster. See
[`examples/envoy`](https://github.com/huyz0/envoy-osproxy/tree/main/examples/envoy).

## Trying it out with no code

The built-in `ReferenceTenancy` is the "works out of the box" default, header- or
mTLS-principal partitioning over a dedicated cluster or a shared index, configured
entirely from the Envoy `filter_config` blob. The live tests
(`crates/evoxy-extproc/tests/e2e.rs`, `tests/e2e_module.rs`) run exactly this
against a real OpenSearch and are the best worked reference.

## How you change it

Tenancy logic is compiled into the artifact (ADR-003): to change it you rebuild
the `.so` / the server and redeploy, static, not a runtime plugin. Runtime knobs
(diagnostics directives, the decision header) flip via the admin surface without a
restart.
