//! Async write mode (ext_proc only): a `202`-and-fan-out write path.
//!
//! Standalone osproxy has an async write mode (ADR-010): the client sends a write,
//! gets an immediate `202 Accepted`, and the durable write happens off the request
//! path. evoxy offers the same on the ext_proc backend, opt-in per request via the
//! [`WRITE_MODE_HEADER`]. When asked, the service runs the filter transform as
//! usual, then produces the *physical* (transformed) request to a fan-out sink with
//! a **durable, acknowledged** produce ([`Bridge::forward_acked`]) before replying
//! `202`. The synchronous write to OpenSearch is skipped.
//!
//! The single hard rule is honesty: a `202` promises the write will happen, so we
//! only send it after the broker acknowledges. If the produce is not acknowledged,
//! we refuse with `503` rather than lie. This mode is ext_proc-only: it needs a real
//! async runtime to await the ack, which the dynamic module has no clean way to do.
//!
//! The filter brain stays pure (ADR-002/004): it transforms, it never produces. The
//! produce lives here, over the same [`AckProducer`] seam osproxy's `KrafkaProducer`
//! implements, so a deployment plugs in a real broker with one line.

use std::future::Future;
use std::pin::Pin;

use envoy_types::pb::envoy::r#type::v3::HttpStatus;
use evoxy_bridge::Bridge;
use osproxy_kafka::{AckProducer, ProduceError};

use crate::extproc::processing_response::Response as Resp;
use crate::extproc::{ImmediateResponse, ProcessingResponse};
use crate::wrap;

/// The request header that opts a write into async mode. Its value must be `async`;
/// any other value (or absence) is a normal synchronous request.
pub const WRITE_MODE_HEADER: &str = "x-evoxy-write-mode";

/// The [`WRITE_MODE_HEADER`] value that selects async mode.
pub(crate) const ASYNC_MODE: &str = "async";

/// Whether the request headers ask for async write mode.
pub(crate) fn wants_async(headers: &[(String, String)]) -> bool {
    headers.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case(WRITE_MODE_HEADER) && v.eq_ignore_ascii_case(ASYNC_MODE)
    })
}

/// The durable fan-out sink an async write is produced to. Object-safe (returns a
/// boxed future) so [`ExtProcService`](crate::ExtProcService) holds a
/// `dyn AsyncWriteSink` and stays generic over the tenancy router alone. The one
/// method must not resolve until the record is **acknowledged** — that ack is what
/// makes the `202` truthful.
pub trait AsyncWriteSink: Send + Sync {
    /// Produce the transformed request (`path` the Kafka key, `body` the payload)
    /// and resolve only once the broker has acknowledged it.
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

/// Any [`Bridge`] over an [`AckProducer`] is an async-write sink: it already awaits
/// the broker ack ([`Bridge::forward_acked`]).
impl<P: AckProducer + Send + Sync> AsyncWriteSink for Bridge<P> {
    fn produce_acked<'a>(
        &'a self,
        path: &'a str,
        body: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(self.forward_acked(path, body))
    }
}

/// The `202 Accepted` reply: the write is durably queued, not yet applied. `op_id`
/// is a shape-only correlation handle (a hash of the physical request, no tenant
/// values) the client can log; there is no proxy-side status endpoint for it.
pub(crate) fn accepted(op_id: &str) -> ProcessingResponse {
    immediate(
        202,
        format!("{{\"status\":\"accepted\",\"op_id\":\"{op_id}\"}}"),
    )
}

/// `503`: async was requested but the write could not be durably queued (the broker
/// did not acknowledge). Refuse rather than send a `202` we cannot back.
pub(crate) fn fanout_unavailable() -> ProcessingResponse {
    immediate(503, r#"{"error":"fanout_unavailable"}"#.to_owned())
}

/// `503`: async was requested but this service has no fan-out sink configured, so it
/// cannot honor async mode. Refuse rather than silently fall back to a sync write.
pub(crate) fn async_unavailable() -> ProcessingResponse {
    immediate(503, r#"{"error":"async_write_unavailable"}"#.to_owned())
}

/// `400`: async mode is meaningful only for writes; a read cannot be `202`-queued.
pub(crate) fn read_unsupported() -> ProcessingResponse {
    immediate(
        400,
        r#"{"error":"async_write_read_unsupported"}"#.to_owned(),
    )
}

/// A shape-only correlation handle for an accepted write: a stable hex hash of the
/// physical request line and body. No tenant value is echoed.
pub(crate) fn op_id(path: &str, body: &[u8]) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hasher);
    body.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Build a shape-only immediate response with a JSON body.
fn immediate(status: u16, body: String) -> ProcessingResponse {
    wrap(Resp::ImmediateResponse(ImmediateResponse {
        status: Some(HttpStatus {
            code: i32::from(status),
        }),
        body: body.into_bytes(),
        ..Default::default()
    }))
}
