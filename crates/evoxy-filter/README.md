# evoxy-filter

The brain, with no dependency on any Envoy SDK. This is what both backends drive.

`Filter::handle` takes one `FilterRequest`, runs it through the adapter and the
route logic, and issues the resulting effects through an `EnvoyActions` trait:
rewrite the method or path, set or remove a header, replace the body, or send a
fail-closed reply. `Filter::shape_response` handles the response phase. Because the
effects go through a trait, the ext_proc service implements it over its gRPC
response, the dynamic module implements it over Envoy's handle, and the tests
implement it with a recorder. The brain is the same in every case.

The crate also ships `ReferenceTenancy`, the built-in tenancy that makes a runnable
filter with no user code. It partitions by a request header or the mTLS principal,
and does dedicated-cluster or shared-index isolation, all from a `FilterConfig`
parsed out of Envoy's filter-config blob.

A panic in the request path can crash an Envoy worker when this runs in the dynamic
module, so the crate holds to the workspace no-panic rule: no `unwrap`, no `expect`,
fail closed on the unexpected.
