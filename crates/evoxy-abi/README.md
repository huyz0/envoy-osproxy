# evoxy-abi

The Envoy-facing wire model. This is the leaf crate: it has no I/O and no
dependency on any other crate in the workspace, so both backends can decode into
the same types.

`FilterRequest` is what a filter receives from Envoy: the method, `:path`,
authority, protocol version, headers, body, and the Envoy-validated `MtlsIdentity`.
`FilterResponse` is an immediate reply the brain can send back without forwarding.
The ext_proc service and the dynamic module both build these same structs, so
everything above this crate is transport-agnostic.

The crate also parses the two pieces of identity Envoy hands over as headers: the
mTLS principal from the `x-forwarded-client-cert` (XFCC) header, and the W3C trace
id from `traceparent`. Both are pure parsing with no allocation beyond the result.
