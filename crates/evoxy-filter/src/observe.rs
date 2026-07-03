//! The backend-neutral observability and admin surface (M7).
//!
//! The reserved introspection paths (`/_evoxy/metrics`, `/_evoxy/explain/<path>`,
//! `/_evoxy/admin/directives`) and the `x-evoxy-decision` response header are the
//! same on either backend: a path check yields a shape-only reply, or a request
//! yields a header value. They are a property of the brain, not of one wire, so they
//! live here and both backends ([`crate::Filter`]'s ext_proc and dynamic-module
//! hosts) delegate to [`Observe`]. Everything is shape-only (counts, cluster ids,
//! decision kinds), never a tenant value, so `/metrics` is safe to leave on in
//! production while `/explain` and the directive plane are the break-glass surfaces.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use evoxy_abi::{FilterRequest, HttpVersion, MtlsIdentity};
use osproxy_tenancy::Router;

use crate::Filter;

/// The reserved path served with a shape-only metrics snapshot.
pub const METRICS_PATH: &str = "/_evoxy/metrics";
/// The reserved path the token-gated directive plane is served on.
pub const ADMIN_PATH: &str = "/_evoxy/admin/directives";
/// The reserved prefix: `/_evoxy/explain/<target path>` explains how `<target>`
/// would route without forwarding it.
pub const EXPLAIN_PREFIX: &str = "/_evoxy/explain";
/// The shape-only routing-decision response header.
pub const DECISION_HEADER: &str = "x-evoxy-decision";

/// A backend-neutral immediate reply: a status and a shape-only JSON body. Each
/// backend renders it (ext_proc as an `ImmediateResponse`, the module via
/// `send_response`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImmediateReply {
    /// The HTTP status to answer with.
    pub status: u16,
    /// The shape-only JSON body.
    pub body: Vec<u8>,
}

/// Process-lifetime request counters. Cheap relaxed atomics on the hot path.
#[derive(Debug, Default)]
pub struct Metrics {
    routed: AtomicU64,
    rejected: AtomicU64,
}

impl Metrics {
    /// Record a request forwarded upstream (a happy-path routing decision).
    pub fn record_routed(&self) {
        self.routed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a request answered with a fail-closed immediate response
    /// (unresolved partition, isolation reject, stale-epoch, over-cap, ...).
    pub fn record_rejected(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
    }

    /// A shape-only JSON snapshot: total, routed, rejected. No tenant values.
    #[must_use]
    pub fn snapshot_json(&self) -> Vec<u8> {
        let routed = self.routed.load(Ordering::Relaxed);
        let rejected = self.rejected.load(Ordering::Relaxed);
        let total = routed + rejected;
        format!("{{\"requests\":{total},\"routed\":{routed},\"rejected\":{rejected}}}").into_bytes()
    }
}

/// Process-lifetime runtime directives, shared across requests: the "act" half of
/// observe-then-act. Today it carries one knob, whether the shape-only decision
/// header is emitted, flippable at runtime via the admin path without a restart. A
/// behavior toggle, never a security policy.
#[derive(Debug)]
pub struct Directives {
    emit_decision: AtomicBool,
}

impl Default for Directives {
    fn default() -> Self {
        Self::new(true)
    }
}

impl Directives {
    /// Directives with the given initial decision-header state.
    #[must_use]
    pub fn new(emit_decision: bool) -> Self {
        Self {
            emit_decision: AtomicBool::new(emit_decision),
        }
    }

    /// Whether the shape-only decision header should be emitted right now.
    #[must_use]
    pub fn emit_decision(&self) -> bool {
        self.emit_decision.load(Ordering::Relaxed)
    }

    /// Apply the settings named in a URL query string (`emit_decision=false`),
    /// ignoring unknown keys. Returns the number of directives changed.
    pub fn apply_query(&self, query: &str) -> usize {
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
    #[must_use]
    pub fn snapshot_json(&self) -> Vec<u8> {
        format!("{{\"emit_decision\":{}}}", self.emit_decision()).into_bytes()
    }
}

/// The reserved-schema config for the observe surface, read from the same
/// `filter_config` blob as the tenancy (unknown keys ignored, so they coexist). It
/// is how a config-only deployment enables the directive plane.
#[derive(Debug, Clone)]
pub struct ObserveConfig {
    /// The bearer token that gates `/_evoxy/admin/directives`. `None` (the default)
    /// leaves the plane fail-closed `403`.
    pub admin_token: Option<String>,
    /// The initial state of the decision header (default on).
    pub emit_decision: bool,
}

impl Default for ObserveConfig {
    fn default() -> Self {
        Self {
            admin_token: None,
            emit_decision: true,
        }
    }
}

impl ObserveConfig {
    /// Parse the reserved observe keys (`admin_token`, `emit_decision`) from a JSON
    /// blob, falling back to defaults for any missing key. Other keys (the tenancy
    /// config) are ignored, so one blob configures both.
    #[must_use]
    pub fn from_json(raw: &str) -> Self {
        let v: serde_json::Value = serde_json::from_str(raw).unwrap_or(serde_json::Value::Null);
        let d = Self::default();
        Self {
            admin_token: v
                .get("admin_token")
                .and_then(serde_json::Value::as_str)
                .map(ToOwned::to_owned),
            emit_decision: v
                .get("emit_decision")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(d.emit_decision),
        }
    }
}

/// The shared observe surface both backends delegate to: the metrics counters, the
/// runtime directives, and the admin token that gates the directive plane.
#[derive(Debug)]
pub struct Observe {
    metrics: Metrics,
    directives: Directives,
    admin_token: Option<String>,
}

impl Default for Observe {
    fn default() -> Self {
        Self::from_config(&ObserveConfig::default())
    }
}

impl Observe {
    /// Build the surface from its config (the directive plane's token and the initial
    /// decision-header state).
    #[must_use]
    pub fn from_config(config: &ObserveConfig) -> Self {
        Self {
            metrics: Metrics::default(),
            directives: Directives::new(config.emit_decision),
            admin_token: config.admin_token.clone(),
        }
    }

    /// Set the directive-plane token (the server-code enable path for a backend with
    /// no config blob, e.g. ext_proc).
    #[must_use]
    pub fn with_admin_token(mut self, token: impl Into<String>) -> Self {
        self.admin_token = Some(token.into());
        self
    }

    /// Record a request forwarded upstream.
    pub fn record_routed(&self) {
        self.metrics.record_routed();
    }

    /// Record a request answered with a fail-closed immediate reply.
    pub fn record_rejected(&self) {
        self.metrics.record_rejected();
    }

    /// The reply for a reserved introspection path, or `None` for a normal
    /// data-plane request. Answers `/metrics` (shape-only counters), `/explain/...`
    /// (a routing dry-run), and `/admin/directives` (token-gated runtime act).
    pub async fn reserved_reply<R: Router>(
        &self,
        filter: &Filter<R>,
        headers: &[(String, String)],
    ) -> Option<ImmediateReply> {
        let path = reserved_path(headers);
        if path == METRICS_PATH {
            return Some(ImmediateReply {
                status: 200,
                body: self.metrics.snapshot_json(),
            });
        }
        if path == ADMIN_PATH {
            return Some(self.admin_reply(headers));
        }
        if let Some(target) = explain_target(headers) {
            let req = request_from_headers(headers, Some(&target));
            return Some(ImmediateReply {
                status: 200,
                body: filter.explain(&req).await.into_bytes(),
            });
        }
        None
    }

    /// The shape-only decision header value for a response, or `None` when the
    /// directive plane has silenced it (or the request does not resolve).
    pub async fn decision_header<R: Router>(
        &self,
        filter: &Filter<R>,
        headers: &[(String, String)],
    ) -> Option<String> {
        if !self.directives.emit_decision() {
            return None;
        }
        let req = request_from_headers(headers, None);
        filter.decision_shape(&req).await
    }

    /// The token-gated directive-plane reply: apply any directives named in the
    /// query, then return the current snapshot. Requires `Authorization: Bearer
    /// <token>` matching the configured token; without a configured token, or on a
    /// mismatch, it fails closed `403`.
    fn admin_reply(&self, headers: &[(String, String)]) -> ImmediateReply {
        let authorized = self.admin_token.as_deref().is_some_and(|token| {
            bearer(headers).is_some_and(|got| constant_time_eq(got.as_bytes(), token.as_bytes()))
        });
        if !authorized {
            return ImmediateReply {
                status: 403,
                body: br#"{"error":"unauthorized"}"#.to_vec(),
            };
        }
        if let Some(query) = raw_query(headers) {
            self.directives.apply_query(query);
        }
        ImmediateReply {
            status: 200,
            body: self.directives.snapshot_json(),
        }
    }
}

/// Build a [`FilterRequest`] from `(name, value)` headers, optionally overriding the
/// `:path` (to explain a different target). Trusts Envoy's XFCC for identity.
fn request_from_headers(
    headers: &[(String, String)],
    path_override: Option<&str>,
) -> FilterRequest {
    let get = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    };
    let identity = get("x-forwarded-client-cert")
        .map(|xfcc| MtlsIdentity::from_xfcc(&xfcc))
        .unwrap_or_default();
    let path = path_override
        .map(ToOwned::to_owned)
        .or_else(|| get(":path"))
        .unwrap_or_default();
    FilterRequest {
        method: get(":method").unwrap_or_default(),
        path_and_query: path,
        authority: get(":authority").unwrap_or_default(),
        version: HttpVersion::Http2,
        headers: headers.to_vec(),
        body: Vec::new(),
        identity,
    }
}

/// The request `:path` with the query stripped, for the reserved-path check.
fn reserved_path(headers: &[(String, String)]) -> &str {
    headers
        .iter()
        .find(|(k, _)| k == ":path")
        .map_or("", |(_, v)| v.split('?').next().unwrap_or(""))
}

/// The target path an explain request names, or `None`. `/_evoxy/explain/o/_search`
/// → `/o/_search`.
fn explain_target(headers: &[(String, String)]) -> Option<String> {
    reserved_path(headers)
        .strip_prefix(EXPLAIN_PREFIX)
        .filter(|rest| rest.starts_with('/'))
        .map(str::to_owned)
}

/// The raw `?query` of the request `:path`, for the directive plane's settings.
fn raw_query(headers: &[(String, String)]) -> Option<&str> {
    headers
        .iter()
        .find(|(k, _)| k == ":path")
        .and_then(|(_, v)| v.split_once('?'))
        .map(|(_, query)| query)
}

/// The bearer token from `Authorization: Bearer <token>` (case-insensitive scheme).
fn bearer(headers: &[(String, String)]) -> Option<&str> {
    let auth = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
        .map(|(_, v)| v.as_str())?;
    let (scheme, token) = auth.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then_some(token.trim())
}

/// Parse a permissive boolean (`true`/`false`/`1`/`0`).
fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "true" | "1" => Some(true),
        "false" | "0" => Some(false),
        _ => None,
    }
}

/// Constant-time byte-equality for the admin bearer token, so a wrong token cannot
/// be recovered by timing. Not crypto (a plain compare), so it pulls no crypto crate
/// — the shipped extension stays crypto-free (ADR-006).
#[must_use]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
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
#[path = "observe_tests.rs"]
mod observe_tests;
