//! Envoy filter-facing wire model.
//!
//! This is the **Envoy side** of the boundary: the decoded HTTP request an
//! `ext_proc` service or a dynamic-module filter receives from a stock Envoy,
//! and the immediate response it can hand back. It is deliberately a leaf crate
//! with no dependency on the reused osproxy brain — [`evoxy-adapter`](../evoxy_adapter/index.html)
//! is the one seam that maps this into an `osproxy_spi::RequestCtx` (docs/00 §2).
//!
//! Nothing here knows about tenancy, routing, or OpenSearch semantics; it only
//! models bytes-as-Envoy-presents-them. Both extension mechanisms (`ext_proc`
//! over gRPC, dynamic module over the C ABI) decode into the *same* types, which
//! is what makes the backend a deployment knob rather than a rewrite (docs/00 §3).
#![deny(missing_docs)]

/// The HTTP version Envoy negotiated with the downstream client. Envoy owns TLS
/// and codec, so we are told this rather than parsing it off the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpVersion {
    /// HTTP/1.1 (cleartext or over TLS terminated by Envoy).
    Http11,
    /// HTTP/2.
    Http2,
}

/// A downstream identity Envoy validated during mTLS and forwarded to the
/// filter (e.g. via `x-forwarded-client-cert` / filter metadata). Because Envoy
/// terminates the client certificate, the filter trusts this rather than parsing
/// a certificate itself (docs/00 §5.4).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MtlsIdentity {
    /// Whether a client certificate was presented and validated at all.
    pub presented: bool,
    /// The certificate subject distinguished name, if any.
    pub subject: String,
    /// URI Subject Alternative Names (e.g. a SPIFFE id), in presentation order.
    pub uri_sans: Vec<String>,
}

impl MtlsIdentity {
    /// The stable principal string to key tenancy on: the first URI SAN if
    /// present (SPIFFE-style identity), else the subject DN. Empty when no
    /// certificate was presented.
    #[must_use]
    pub fn stable_id(&self) -> &str {
        if let Some(san) = self.uri_sans.first() {
            san
        } else {
            &self.subject
        }
    }
}

/// The decoded request an Envoy filter processes. Headers and body are owned
/// because the filter may mutate and forward them; ownership also lets an
/// adapter build a borrowing `RequestCtx` over stable storage.
#[derive(Debug, Clone)]
pub struct FilterRequest {
    /// The HTTP method verbatim (`GET`, `PUT`, `POST`, `DELETE`, `HEAD`).
    pub method: String,
    /// The request target, path plus optional `?query` (the HTTP/2 `:path`).
    pub path_and_query: String,
    /// The `:authority` / `Host` header value.
    pub authority: String,
    /// The negotiated HTTP version.
    pub version: HttpVersion,
    /// Request headers in presentation order (lower-cased names, as Envoy emits).
    pub headers: Vec<(String, String)>,
    /// The (possibly buffered or streamed) request body.
    pub body: Vec<u8>,
    /// The Envoy-validated downstream identity, if mTLS was in play.
    pub identity: MtlsIdentity,
}

impl FilterRequest {
    /// The path component, with any `?query` stripped.
    #[must_use]
    pub fn path(&self) -> &str {
        match self.path_and_query.split_once('?') {
            Some((path, _)) => path,
            None => &self.path_and_query,
        }
    }

    /// The raw query string (without the leading `?`), if present.
    #[must_use]
    pub fn query(&self) -> Option<&str> {
        self.path_and_query.split_once('?').map(|(_, q)| q)
    }

    /// A header value by case-insensitive name (first match). Envoy lower-cases
    /// header names, but callers should not have to assume that.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// Whether this looks like a gRPC call (so the adapter classes it as
    /// [`Protocol::Grpc`](../evoxy_adapter/index.html)).
    #[must_use]
    pub fn is_grpc(&self) -> bool {
        self.header("content-type")
            .is_some_and(|ct| ct.starts_with("application/grpc"))
    }
}

/// An immediate response a filter returns to Envoy instead of forwarding
/// upstream (e.g. a fail-closed isolation rejection). Envoy owns the wire, so we
/// only supply status, headers, and body.
#[derive(Debug, Clone)]
pub struct FilterResponse {
    /// The HTTP status code.
    pub status: u16,
    /// Response headers in presentation order.
    pub headers: Vec<(String, String)>,
    /// The response body.
    pub body: Vec<u8>,
}

impl FilterResponse {
    /// A JSON response with the given status and body.
    #[must_use]
    pub fn json(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            body,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(path_and_query: &str) -> FilterRequest {
        FilterRequest {
            method: "GET".to_owned(),
            path_and_query: path_and_query.to_owned(),
            authority: "os.local".to_owned(),
            version: HttpVersion::Http2,
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            body: Vec::new(),
            identity: MtlsIdentity::default(),
        }
    }

    #[test]
    fn splits_path_and_query() {
        let r = req("/orders/_search?scroll=1m");
        assert_eq!(r.path(), "/orders/_search");
        assert_eq!(r.query(), Some("scroll=1m"));
    }

    #[test]
    fn path_without_query() {
        let r = req("/orders/_doc/42");
        assert_eq!(r.path(), "/orders/_doc/42");
        assert_eq!(r.query(), None);
    }

    #[test]
    fn header_lookup_is_case_insensitive() {
        let r = req("/");
        assert_eq!(r.header("Content-Type"), Some("application/json"));
        assert_eq!(r.header("x-missing"), None);
    }

    #[test]
    fn grpc_detected_by_content_type() {
        let mut r = req("/");
        assert!(!r.is_grpc());
        r.headers[0].1 = "application/grpc+proto".to_owned();
        assert!(r.is_grpc());
    }

    #[test]
    fn stable_id_prefers_uri_san_then_subject() {
        let mut id = MtlsIdentity {
            presented: true,
            subject: "CN=svc".to_owned(),
            uri_sans: vec!["spiffe://td/svc".to_owned()],
        };
        assert_eq!(id.stable_id(), "spiffe://td/svc");
        id.uri_sans.clear();
        assert_eq!(id.stable_id(), "CN=svc");
    }
}
