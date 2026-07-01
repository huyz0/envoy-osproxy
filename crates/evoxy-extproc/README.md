# evoxy-extproc

The ext_proc backend: an Envoy External Processing gRPC service over the shared
brain. Envoy sends each request's phases over gRPC, and this service returns the
header and body mutations Envoy applies. It is pure Rust over `tonic`, with no
libclang or Envoy headers to build.

`ExtProcService::new(filter)` builds the service. It has a request-body cap that
refuses an oversized body with `413` before buffering it, and an optional bearer
token that gates the runtime directive plane. `ExternalProcessorServer` is the
generated tonic wrapper you mount on a server, so a binary is a few lines. See
[Building the ext_proc backend](../../docs/guide/03-build-extproc.md).

Alongside the data path, the service answers a few reserved paths on Envoy's own
port as immediate responses, so there is no second server: `/_evoxy/metrics` for
shape-only counters, `/_evoxy/explain/...` for a dry-run of how a request would
route, and `/_evoxy/admin/directives` to flip runtime diagnostics behind the token.

`ExtProcService` is generic over the tenancy router, so a custom `TenancySpi` runs
here the same as the reference one. This works because the osproxy SPI returns
`Send` futures (osproxy 1.0.2), which the gRPC response stream requires.
