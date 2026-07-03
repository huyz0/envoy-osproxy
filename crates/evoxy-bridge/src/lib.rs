//! The async fan-out **bridge** (ADR-005): turn an Envoy-mirrored request into a
//! Kafka record.
//!
//! ADR-005 decides async fan-out is Envoy `request_mirror_policies` shadowing the
//! (already transformed) request to a dedicated HTTP→Kafka bridge, *not* an
//! in-filter produce — an Envoy extension cannot cleanly produce to Kafka. This
//! crate is that bridge's core: it receives the physical request Envoy mirrored
//! (its method, path, and body, as the filter transformed them) and produces it as
//! one record over osproxy's [`Producer`] seam. Because it is the *same* seam
//! osproxy's `KrafkaProducer` implements, the deployment binary swaps the recording
//! [`InMemoryProducer`](osproxy_kafka::InMemoryProducer) for the real broker client
//! with one line — nothing here links a broker or any crypto.
//!
//! This is a **separate deployment artifact**, not the Envoy extension: the filter
//! stays pure (ADR-002), Envoy mirrors, this bridge produces. The HTTP-receive
//! front (a small server) is deployment glue over [`Bridge::forward`]; the
//! Envoy→bridge hop itself is proven live in `evoxy-extproc`'s `mirror` e2e.
#![deny(missing_docs)]

use osproxy_kafka::{AckProducer, ProduceError, Producer};

/// The record key/payload a mirrored request produces to Kafka.
///
/// - **key**: the request path (the physical `/{index}/_doc/{id}`), so records for
///   the same document land on the same partition — preserving per-document order,
///   the property a downstream replayer needs.
/// - **payload**: the request body, verbatim, as the filter transformed it
///   (injected tenancy fields and constructed ids already applied).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FanoutRecord {
    /// The Kafka partition/order key: the physical request path.
    pub key: Vec<u8>,
    /// The Kafka payload: the transformed request body.
    pub payload: Vec<u8>,
}

/// The fan-out bridge over a [`Producer`]. Generic so the deployment plugs in
/// `KrafkaProducer` and tests plug in `InMemoryProducer`.
#[derive(Debug, Clone)]
pub struct Bridge<P> {
    producer: P,
    topic: String,
}

impl<P> Bridge<P> {
    /// A bridge that produces mirrored requests to `topic`.
    pub fn new(producer: P, topic: impl Into<String>) -> Self {
        Self {
            producer,
            topic: topic.into(),
        }
    }

    /// The Kafka record a mirrored request maps to (pure; no I/O).
    #[must_use]
    pub fn record(path: &str, body: &[u8]) -> FanoutRecord {
        FanoutRecord {
            key: path.as_bytes().to_vec(),
            payload: body.to_vec(),
        }
    }

    /// The producer this bridge writes through (for introspection/tests).
    pub fn producer(&self) -> &P {
        &self.producer
    }
}

impl<P: Producer> Bridge<P> {
    /// Produce one Envoy-mirrored request as a fan-out record. Fire-and-forget from
    /// the caller's view (the [`Producer`] owns durability/retry) — the default
    /// tier, matching the mirror's fire-and-forget semantics (ADR-005).
    ///
    /// # Errors
    /// [`ProduceError`] if the record could not be enqueued.
    pub fn forward(&self, path: &str, body: &[u8]) -> Result<(), ProduceError> {
        let record = Self::record(path, body);
        self.producer
            .produce(&self.topic, &record.key, &record.payload)
    }
}

/// A [`Bridge`] over an [`AckProducer`] is an `evoxy_filter::AsyncWriteSink`: it
/// already awaits the broker ack, so a backend can drive an async write (ADR-010)
/// straight through it. Lets the ext_proc service and the dynamic module answer `202`
/// only after a durable produce.
impl<P: AckProducer> evoxy_filter::AsyncWriteSink for Bridge<P> {
    fn produce_acked<'a>(
        &'a self,
        path: &'a str,
        body: &'a [u8],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ProduceError>> + Send + 'a>>
    {
        Box::pin(self.forward_acked(path, body))
    }
}

impl<P: AckProducer> Bridge<P> {
    /// Produce one mirrored request and **await the broker acknowledgement** — the
    /// durable tier (the seam a spill buffer drains onto, since it must confirm
    /// delivery before dropping a record). Heavier than [`forward`](Bridge::forward);
    /// use it when fan-out loss is unacceptable and the caller can wait.
    ///
    /// # Errors
    /// [`ProduceError`] if the record was not acknowledged.
    pub async fn forward_acked(&self, path: &str, body: &[u8]) -> Result<(), ProduceError> {
        let record = Bridge::<P>::record(path, body);
        self.producer
            .send_acked(&self.topic, &record.key, &record.payload)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use osproxy_kafka::InMemoryProducer;

    #[test]
    fn forwards_the_transformed_request_as_a_record() {
        let bridge = Bridge::new(InMemoryProducer::new(), "evoxy.fanout");
        // The physical request Envoy would mirror after the filter transformed it.
        let path = "/orders_shared/_doc/acme%3A1";
        let body = br#"{"_tenant":"acme","id":1,"who":"acme"}"#;

        bridge.forward(path, body).unwrap();

        let produced = bridge.producer().produced();
        assert_eq!(produced.len(), 1);
        let (topic, key, payload) = &produced[0];
        assert_eq!(topic, "evoxy.fanout");
        // Keyed by the physical path so a document's records keep their order.
        assert_eq!(key, path.as_bytes());
        // Payload is the transformed body verbatim (tenancy field already injected).
        assert_eq!(payload, body);
    }

    #[test]
    fn record_is_pure_and_matches_forward() {
        let record = Bridge::<InMemoryProducer>::record("/o/_doc/1", b"{}");
        assert_eq!(record.key, b"/o/_doc/1");
        assert_eq!(record.payload, b"{}");
    }

    /// One acknowledged record: `(topic, key, payload)`.
    type Acked = (String, Vec<u8>, Vec<u8>);

    /// A recording [`AckProducer`] for the durable-tier test — records the record
    /// and reports success, as a broker that acknowledged would.
    #[derive(Default)]
    struct RecordingAck {
        acked: std::sync::Mutex<Vec<Acked>>,
    }

    impl AckProducer for RecordingAck {
        async fn send_acked(
            &self,
            topic: &str,
            key: &[u8],
            payload: &[u8],
        ) -> Result<(), ProduceError> {
            self.acked
                .lock()
                .unwrap()
                .push((topic.to_owned(), key.to_vec(), payload.to_vec()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn forward_acked_awaits_the_broker_ack() {
        let bridge = Bridge::new(RecordingAck::default(), "evoxy.fanout");
        bridge
            .forward_acked("/orders_shared/_doc/acme%3A1", br#"{"_tenant":"acme"}"#)
            .await
            .unwrap();

        let acked = bridge.producer().acked.lock().unwrap();
        assert_eq!(acked.len(), 1);
        assert_eq!(acked[0].0, "evoxy.fanout");
        assert_eq!(acked[0].1, b"/orders_shared/_doc/acme%3A1");
    }
}
