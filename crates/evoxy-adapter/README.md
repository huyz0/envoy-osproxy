# evoxy-adapter

The seam between Envoy and the reused engine. This one crate is the whole port: it
turns a `FilterRequest` into the `RequestCtx` the osproxy engine consumes.

`RequestParts::from_filter` extracts the owned facets of a request once. It
classifies the path into an endpoint kind (get-by-id, search, bulk, and so on) and
derives the principal from the mTLS identity. `RequestParts::ctx()` then builds the
borrowing `RequestCtx` the engine reads. Splitting extraction from borrowing keeps
the hot path allocation-light and lets the same parts back both the request and the
response phase.

The adapter maps Envoy to the engine and back. It makes no policy decisions;
tenancy and routing live in the engine. It fails closed on anything it cannot map,
such as an unsupported method.
