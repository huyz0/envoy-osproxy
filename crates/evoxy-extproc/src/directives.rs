//! Runtime directives — the "act" half of observe-then-act (M7).
//!
//! A small, shape-only control surface an operator (or an agent) flips at runtime
//! without a restart, via the token-gated `/_evoxy/admin/directives` reserved
//! path. Today it carries one knob: whether the shape-only decision header is
//! emitted. Per-instance, relaxed atomics — the same posture as `/metrics`. The
//! directive is a *behavior* toggle, never a security policy (those are set at
//! deploy time, not flipped over the wire).

use std::sync::atomic::{AtomicBool, Ordering};

/// The reserved admin path the directive plane is served on.
pub(crate) const ADMIN_PATH: &str = "/_evoxy/admin/directives";

/// Process-lifetime runtime directives, shared across streams.
#[derive(Debug)]
pub(crate) struct Directives {
    emit_decision: AtomicBool,
}

impl Default for Directives {
    fn default() -> Self {
        // The decision header is on by default (M7b); an operator can silence it.
        Self {
            emit_decision: AtomicBool::new(true),
        }
    }
}

impl Directives {
    /// Whether the shape-only decision header should be emitted right now.
    pub(crate) fn emit_decision(&self) -> bool {
        self.emit_decision.load(Ordering::Relaxed)
    }

    /// Apply the settings named in a URL query string (`emit_decision=false`),
    /// ignoring unknown keys. Returns the number of directives changed.
    pub(crate) fn apply_query(&self, query: &str) -> usize {
        let mut applied = 0;
        for (key, value) in query.split('&').filter_map(|p| p.split_once('=')) {
            if key == "emit_decision" {
                if let Some(on) = parse_bool(value) {
                    self.emit_decision.store(on, Ordering::Relaxed);
                    applied += 1;
                }
            }
        }
        applied
    }

    /// A shape-only JSON snapshot of the current directives.
    pub(crate) fn snapshot_json(&self) -> Vec<u8> {
        format!("{{\"emit_decision\":{}}}", self.emit_decision()).into_bytes()
    }
}

/// Parse a permissive boolean (`true`/`false`/`1`/`0`).
fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

/// Constant-time byte-equality for the admin bearer token, so a wrong token
/// cannot be recovered by timing. Not crypto (a plain compare), so it does not
/// pull a crypto crate — the shipped extension stays crypto-free (ADR-006).
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_query_flips_emit_decision() {
        let d = Directives::default();
        assert!(d.emit_decision());
        assert_eq!(d.apply_query("emit_decision=false&unknown=x"), 1);
        assert!(!d.emit_decision());
        assert_eq!(
            String::from_utf8(d.snapshot_json()).unwrap(),
            r#"{"emit_decision":false}"#
        );
    }

    #[test]
    fn apply_query_ignores_unknown_and_malformed() {
        let d = Directives::default();
        assert_eq!(d.apply_query("emit_decision=maybe&other=1"), 0);
        assert!(d.emit_decision()); // unchanged
    }

    #[test]
    fn constant_time_eq_matches_only_equal() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secrez"));
        assert!(!constant_time_eq(b"secret", b"secre"));
    }
}
