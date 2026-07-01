//! Shape-only request metrics for the introspection plane (M7).
//!
//! The one introspection surface meant to stay on in production: per-instance
//! counters of how many requests this service saw and how they resolved (routed
//! upstream vs. fail-closed). Shape-only — counts, never a tenant value — so it is
//! safe to expose. Served by the filter itself as an immediate response to a
//! reserved path (`/_evoxy/metrics`), so it rides Envoy's own port with no second
//! server. Per-instance by design; a fleet rollup is an external aggregator's job.

use std::sync::atomic::{AtomicU64, Ordering};

/// The reserved path the filter answers with a metrics snapshot instead of
/// forwarding upstream.
pub(crate) const METRICS_PATH: &str = "/_evoxy/metrics";

/// Process-lifetime request counters. Cheap relaxed atomics on the hot path.
#[derive(Debug, Default)]
pub(crate) struct Metrics {
    routed: AtomicU64,
    rejected: AtomicU64,
}

impl Metrics {
    /// Record a request forwarded upstream (a happy-path routing decision).
    pub(crate) fn record_routed(&self) {
        self.routed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a request answered with a fail-closed immediate response
    /// (unresolved partition, isolation reject, stale-epoch, over-cap, …).
    pub(crate) fn record_rejected(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// A shape-only JSON snapshot: total, routed, rejected. No tenant values.
    pub(crate) fn snapshot_json(&self) -> Vec<u8> {
        let routed = self.routed.load(Ordering::Relaxed);
        let rejected = self.rejected.load(Ordering::Relaxed);
        let total = routed + rejected;
        format!("{{\"requests\":{total},\"routed\":{routed},\"rejected\":{rejected}}}").into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_totals_the_outcomes() {
        let m = Metrics::default();
        m.record_routed();
        m.record_routed();
        m.record_rejected();
        let json = String::from_utf8(m.snapshot_json()).unwrap();
        assert_eq!(json, r#"{"requests":3,"routed":2,"rejected":1}"#);
    }

    #[test]
    fn fresh_metrics_are_zero() {
        let json = String::from_utf8(Metrics::default().snapshot_json()).unwrap();
        assert_eq!(json, r#"{"requests":0,"routed":0,"rejected":0}"#);
    }
}
