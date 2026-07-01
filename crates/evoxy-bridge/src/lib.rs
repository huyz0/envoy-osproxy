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

use osproxy_kafka::{ProduceError, Producer};

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

impl<P: Producer> Bridge<P> {
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

    /// Produce one Envoy-mirrored request as a fan-out record. Fire-and-forget from
    /// the caller's view (the [`Producer`] owns durability/retry).
    ///
    /// # Errors
    /// [`ProduceError`] if the record could not be enqueued.
    pub fn forward(&self, path: &str, body: &[u8]) -> Result<(), ProduceError> {
        let record = Self::record(path, body);
        self.producer
            .produce(&self.topic, &record.key, &record.payload)
    }

    /// The producer this bridge writes through (for introspection/tests).
    pub fn producer(&self) -> &P {
        &self.producer
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
}
