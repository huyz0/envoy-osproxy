//! The reference tenancy as a dynamic module with async write mode enabled.
//!
//! The whole cdylib is one [`register_async!`] call: the first factory builds the
//! filter (here the reference tenancy from `filter_config`), the second builds a
//! durable fan-out sink. A write carrying `x-evoxy-write-mode: async` is then
//! transformed, produced to the sink, and answered `202` instead of forwarding.
//!
//! Async write mode needs a durable, *acknowledged* produce (that ack is what makes
//! the `202` truthful), so the sink is a `Bridge` over an
//! [`AckProducer`](osproxy_kafka::AckProducer). This demo uses a trivial always-ack
//! producer so it links no broker; swap in osproxy's real `KrafkaProducer` (its
//! `AckProducer` impl) for production.
//!
//! Caveat: on the dynamic module, awaiting the broker ack blocks the Envoy worker
//! thread the filter runs on. Enable async only when that trade is acceptable; the
//! ext_proc backend blocks only its own sidecar task instead.

use std::sync::Arc;

use evoxy_bridge::Bridge;
use evoxy_module_sdk::{reference_filter, AsyncWriteSink};
use osproxy_kafka::{AckProducer, ProduceError};

/// A demo acknowledging producer: it accepts every record immediately. A real
/// deployment uses `KrafkaProducer`, whose `send_acked` resolves only once the
/// broker has durably accepted the record.
struct DemoAckProducer;

impl AckProducer for DemoAckProducer {
    async fn send_acked(
        &self,
        _topic: &str,
        _key: &[u8],
        _payload: &[u8],
    ) -> Result<(), ProduceError> {
        Ok(())
    }
}

evoxy_module_sdk::register_async!(
    reference_filter,
    // The sink factory sees the same `filter_config` blob as the tenancy. Return
    // `None` to leave async off; here we always wire the demo producer.
    |_config: &str| -> Option<Arc<dyn AsyncWriteSink>> {
        Some(Arc::new(Bridge::new(DemoAckProducer, "evoxy.writes")))
    }
);
