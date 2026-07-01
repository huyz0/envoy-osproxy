# evoxy-route

Transform-then-forward routing. Given a resolved placement, this crate produces the
request mutation Envoy applies, and reshapes the upstream response back to the
client's logical view.

`prepare` is the request-side entry point. It resolves the placement through the
router, then dispatches by endpoint: rewrite a write's path and body, wrap a
search's query with the partition filter, rewrite each item of a `_bulk` NDJSON
body, or demux `_mget` and `_msearch`. It also runs the migration write gate, so a
write held during a cutover is rejected before anything is forwarded.

The response side strips injected fields and maps physical document ids back to the
logical ids the client sent, so a shared-index deployment stays invisible to the
caller. Document ids are percent-encoded when spliced into a path, so an id that
contains a slash or colon does not break the request line.

This crate does not talk to Envoy or the network. The backends drive it and apply
its output.
