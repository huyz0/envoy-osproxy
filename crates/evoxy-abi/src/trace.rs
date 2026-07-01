//! Reading the W3C `traceparent` trace-id (M7).
//!
//! Envoy owns tracing: it generates and propagates the W3C `traceparent` header
//! (`version-traceid-spanid-flags`) and forwards it upstream. The extension does
//! not manage the span — it only *reads* the trace-id so its shape-only signals
//! (the decision header, `/explain`) can be correlated with Envoy's span by an
//! operator. A trace-id is a random 128-bit token, not a tenant value, so it is
//! safe to surface. We never mutate or strip `traceparent`.

use crate::FilterRequest;

/// The trace-id field of a W3C `traceparent`, or `None` if it is malformed. The
/// header is `version-traceid-spanid-flags`; a valid trace-id is 32 lowercase hex
/// digits and not all-zero (the "invalid" sentinel).
#[must_use]
pub fn trace_id_of(traceparent: &str) -> Option<&str> {
    let mut fields = traceparent.split('-');
    let _version = fields.next()?;
    let trace_id = fields.next()?;
    // A well-formed header has exactly four fields; reject a short one.
    fields.next()?;
    fields.next()?;
    let valid = trace_id.len() == 32
        && trace_id.bytes().all(|b| b.is_ascii_hexdigit())
        && trace_id.bytes().any(|b| b != b'0');
    valid.then_some(trace_id)
}

impl FilterRequest {
    /// The W3C trace-id from this request's `traceparent`, if present and valid.
    #[must_use]
    pub fn trace_id(&self) -> Option<&str> {
        self.header("traceparent").and_then(trace_id_of)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TP: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn extracts_the_trace_id() {
        assert_eq!(trace_id_of(TP), Some("4bf92f3577b34da6a3ce929d0e0e4736"));
    }

    #[test]
    fn rejects_malformed_or_all_zero() {
        assert_eq!(trace_id_of(""), None);
        assert_eq!(trace_id_of("00-tooshort-00f067aa0ba902b7-01"), None);
        // The all-zero trace-id is the invalid sentinel.
        assert_eq!(
            trace_id_of("00-00000000000000000000000000000000-00f067aa0ba902b7-01"),
            None
        );
        // A three-field header is not W3C traceparent.
        assert_eq!(trace_id_of("00-4bf92f3577b34da6a3ce929d0e0e4736-01"), None);
    }
}
