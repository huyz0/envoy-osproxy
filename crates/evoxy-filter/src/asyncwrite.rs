//! Async write mode (ADR-010), backend-neutral.
//!
//! Standalone osproxy has an async write mode: the client sends a write, gets an
//! immediate `202 Accepted`, and the durable write happens off the request path.
//! evoxy offers the same, opt-in per request via the [`WRITE_MODE_HEADER`]. The
//! whole decision lives here so both backends share it: the ext_proc service and the
//! dynamic module each just render the returned [`ImmediateReply`].
//!
//! The single hard rule is honesty: a `202` promises the write will happen, so we
//! send it only after the broker acknowledges the produce ([`AsyncWriteSink`]). If it
//! is not acknowledged we refuse with `503` rather than lie. Awaiting that ack blocks
//! the caller, so a backend must be able to afford it (ext_proc blocks its own task;
//! the module blocks an Envoy worker, which is why it enables this only deliberately).

use std::future::Future;
use std::pin::Pin;

use osproxy_tenancy::Router;

use crate::observe::ImmediateReply;
use crate::{EnvoyActions, Filter};

pub use osproxy_kafka::ProduceError;

/// The request header that opts a write into async mode. Its value must be `async`;
/// any other value (or absence) is a normal synchronous request.
pub const WRITE_MODE_HEADER: &str = "x-evoxy-write-mode";

/// The [`WRITE_MODE_HEADER`] value that selects async mode.
const ASYNC_MODE: &str = "async";

/// Whether the request headers ask for async write mode.
#[must_use]
pub fn wants_async(headers: &[(String, String)]) -> bool {
    headers.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case(WRITE_MODE_HEADER) && v.eq_ignore_ascii_case(ASYNC_MODE)
    })
}

/// The durable fan-out sink an async write is produced to. Object-safe (returns a
/// boxed future) so a backend holds a `dyn AsyncWriteSink`. The one method must not
/// resolve until the record is **acknowledged** — that ack is what makes the `202`
/// truthful. Any [`Bridge`](../../evoxy_bridge/struct.Bridge.html) over an
/// `AckProducer` implements it (see `evoxy-bridge`).
pub trait AsyncWriteSink: Send + Sync {
    /// Produce the transformed request (`path` the Kafka key, `body` the payload) and
    /// resolve only once the broker has acknowledged it.
    ///
    /// # Errors
    /// [`ProduceError`] if the record was not acknowledged; the caller then refuses
    /// the write with `503` instead of a false `202`.
    fn produce_acked<'a>(
        &'a self,
        path: &'a str,
        body: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>>;
}

impl<R: Router> Filter<R> {
    /// Run the async-write contract for `req`: transform it, produce the physical
    /// request durably through `sink`, and return the client reply. Refuses rather
    /// than lie — `400` on a read (a read cannot be `202`-queued), `503` when no sink
    /// is configured (never a silent sync downgrade), `503` when the broker does not
    /// acknowledge — and never forwards to OpenSearch. A fail-closed transform
    /// (unresolved/rejected) is surfaced as its own reply, never an accepted write.
    pub async fn async_write(
        &self,
        req: &evoxy_abi::FilterRequest,
        sink: Option<&dyn AsyncWriteSink>,
    ) -> ImmediateReply {
        // Async mode is meaningful only for writes.
        if !self.is_write(req) {
            return reply(400, r#"{"error":"async_write_read_unsupported"}"#);
        }
        // No sink: refuse rather than silently fall back to a sync write.
        let Some(sink) = sink else {
            return reply(503, r#"{"error":"async_write_unavailable"}"#);
        };

        // Seed with the full path+query: the brain's `set_path` carries the query
        // (`?routing=`) when it rewrites, so an untransformed write must keep its
        // query too — the produce key is then consistent across isolation modes.
        let mut capture = CaptureActions::new(req.path_and_query.clone());
        let _decision = self.handle(req, &mut capture).await;
        if let Some((status, body)) = capture.immediate {
            return ImmediateReply { status, body };
        }
        let (path, body) = (capture.path, capture.body);

        // The broker must acknowledge before we answer `202`.
        if sink.produce_acked(&path, &body).await.is_ok() {
            reply(
                202,
                &format!(
                    "{{\"status\":\"accepted\",\"op_id\":\"{}\"}}",
                    op_id(&path, &body)
                ),
            )
        } else {
            reply(503, r#"{"error":"fanout_unavailable"}"#)
        }
    }
}

/// A shape-only correlation handle for an accepted write: a stable hex hash of the
/// physical request line and body. No tenant value is echoed.
fn op_id(path: &str, body: &[u8]) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hasher);
    body.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Build a shape-only immediate reply.
fn reply(status: u16, body: &str) -> ImmediateReply {
    ImmediateReply {
        status,
        body: body.as_bytes().to_vec(),
    }
}

/// An [`EnvoyActions`] recorder for the async path: it captures the transformed
/// request (method/path/body) — which is all a produce needs, no forward happens —
/// or the fail-closed immediate the brain sent. Header ops and routing are ignored:
/// the record is keyed by path with the body as payload.
struct CaptureActions {
    path: String,
    body: Vec<u8>,
    immediate: Option<(u16, Vec<u8>)>,
}

impl CaptureActions {
    /// A recorder seeded with the original path (kept when the brain leaves the
    /// request line unchanged, e.g. a `DedicatedCluster` write).
    fn new(path: String) -> Self {
        Self {
            path,
            body: Vec::new(),
            immediate: None,
        }
    }
}

impl EnvoyActions for CaptureActions {
    fn set_upstream_cluster(&mut self, _cluster: &str) {}
    fn set_upstream_host(&mut self, _host: &str) {}
    // The physical method is not part of the produced record (keyed by path, payload
    // is the body), so the async path does not need it.
    fn set_method(&mut self, _method: &str) {}
    fn set_path(&mut self, path: &str) {
        path.clone_into(&mut self.path);
    }
    fn set_body(&mut self, body: &[u8]) {
        self.body = body.to_vec();
    }
    fn set_header(&mut self, _name: &str, _value: &str) {}
    fn remove_header(&mut self, _name: &str) {}
    fn send_local_reply(&mut self, status: u16, _headers: &[(String, String)], body: &[u8]) {
        self.immediate = Some((status, body.to_vec()));
    }
}

#[cfg(test)]
#[path = "asyncwrite_tests.rs"]
mod asyncwrite_tests;
