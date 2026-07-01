//! The one seam between Envoy and the reused osproxy engine.
//!
//! osproxy already split the wire from the brain: its `osproxy-transport` owns
//! bytes/TLS/pooling, and `osproxy-engine::Pipeline::handle(&RequestCtx)` is a
//! pure, transport-agnostic brain (docs/00 §2). This crate is the whole port
//! thesis made concrete: take an Envoy [`FilterRequest`], build the *exact*
//! [`RequestCtx`] the standalone proxy builds, and hand it to that same engine.
//!
//! It is intentionally the only crate aware of both worlds. Everything upstream
//! ([`evoxy-abi`](../evoxy_abi/index.html)) is Envoy-only; everything downstream
//! (the osproxy crates) is transport-agnostic. Because both extension mechanisms
//! — `ext_proc` and the dynamic module — decode into the same [`FilterRequest`],
//! this seam serves both, making the backend a deployment knob (docs/00 §3).
//!
//! # Example
//! ```
//! use evoxy_abi::{FilterRequest, HttpVersion, MtlsIdentity};
//! use evoxy_adapter::RequestParts;
//! use osproxy_core::EndpointKind;
//!
//! let req = FilterRequest {
//!     method: "PUT".into(),
//!     path_and_query: "/orders/_doc/42".into(),
//!     authority: "os.local".into(),
//!     version: HttpVersion::Http2,
//!     headers: vec![("content-type".into(), "application/json".into())],
//!     body: br#"{"total":10}"#.to_vec(),
//!     identity: MtlsIdentity { presented: true, subject: "CN=svc".into(), uri_sans: vec![] },
//! };
//! let parts = RequestParts::from_filter(&req, "req-1")?;
//! let ctx = parts.ctx();
//! assert_eq!(ctx.endpoint(), EndpointKind::IngestDoc);
//! assert_eq!(ctx.logical_index(), "orders");
//! assert_eq!(ctx.doc_id(), Some("42"));
//! # Ok::<(), evoxy_adapter::AdaptError>(())
//! ```
#![deny(missing_docs)]

mod classify;

pub use classify::{classify, Classified};

use evoxy_abi::FilterRequest;
use osproxy_core::{EndpointKind, PrincipalId, RequestId};
use osproxy_spi::{HeaderView, HttpMethod, Principal, Protocol, RequestCtx};

/// The principal id used when Envoy presented no validated client certificate.
/// A downstream policy may still reject anonymous access; the adapter's job is
/// only faithful mapping, not authorization.
pub const ANONYMOUS: &str = "anonymous";

/// Errors that can arise mapping an Envoy request into a [`RequestCtx`]. The set
/// is small by design: classification never fails (it falls back to
/// [`EndpointKind::Unknown`]), so the only hard error is an unmappable method.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AdaptError {
    /// The HTTP method is not one the proxy handles.
    #[error("unsupported HTTP method: {0}")]
    UnsupportedMethod(String),
}

/// Owned request facets, extracted once from a [`FilterRequest`], over which a
/// borrowing [`RequestCtx`] is built via [`RequestParts::ctx`]. Owning the parts
/// (rather than borrowing the `FilterRequest`) lets the ctx outlive header/body
/// mutation the filter may perform, and keeps the borrow graph simple.
#[derive(Debug, Clone)]
pub struct RequestParts {
    principal: Principal,
    request_id: RequestId,
    method: HttpMethod,
    protocol: Protocol,
    endpoint: EndpointKind,
    logical_index: String,
    doc_id: Option<String>,
    query: Option<String>,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl RequestParts {
    /// Extract the parts from an Envoy request. `request_id` is Envoy's
    /// `x-request-id` (the caller reads it off the filter so the id survives the
    /// whole hop for traceability, docs/05).
    ///
    /// # Errors
    /// [`AdaptError::UnsupportedMethod`] if the method is not GET/PUT/POST/DELETE/HEAD.
    pub fn from_filter(req: &FilterRequest, request_id: &str) -> Result<Self, AdaptError> {
        let method = parse_method(&req.method)?;
        let Classified {
            logical_index,
            endpoint,
            doc_id,
        } = classify(method, req.path());
        Ok(Self {
            principal: principal_of(req),
            request_id: RequestId::from(request_id),
            method,
            protocol: protocol_of(req),
            endpoint,
            logical_index,
            doc_id,
            query: req.query().map(ToOwned::to_owned),
            path: req.path().to_owned(),
            headers: req.headers.clone(),
            body: req.body.clone(),
        })
    }

    /// Build the borrowing [`RequestCtx`] the osproxy engine consumes. The
    /// returned ctx borrows `self`, so keep `self` alive for the pipeline call.
    #[must_use]
    pub fn ctx(&self) -> RequestCtx<'_> {
        RequestCtx::new(
            &self.principal,
            &self.request_id,
            self.method,
            self.endpoint,
            self.protocol,
            &self.logical_index,
            HeaderView::new(&self.headers),
            &self.body,
        )
        .with_doc_id(self.doc_id.as_deref())
        .with_query(self.query.as_deref())
        .with_path(&self.path)
    }

    /// The classified endpoint (exposed so a filter can decide, before building
    /// the ctx, whether it even needs the body — most reads do not, which lets
    /// the `ext_proc` backend skip body streaming, docs/00 §6).
    #[must_use]
    pub fn endpoint(&self) -> EndpointKind {
        self.endpoint
    }
}

/// Map an Envoy method string to the engine's [`HttpMethod`].
fn parse_method(method: &str) -> Result<HttpMethod, AdaptError> {
    match method {
        "GET" => Ok(HttpMethod::Get),
        "PUT" => Ok(HttpMethod::Put),
        "POST" => Ok(HttpMethod::Post),
        "DELETE" => Ok(HttpMethod::Delete),
        "HEAD" => Ok(HttpMethod::Head),
        other => Err(AdaptError::UnsupportedMethod(other.to_owned())),
    }
}

/// gRPC when the content-type says so, else the negotiated HTTP version.
fn protocol_of(req: &FilterRequest) -> Protocol {
    if req.is_grpc() {
        Protocol::Grpc
    } else {
        match req.version {
            evoxy_abi::HttpVersion::Http11 => Protocol::Http1,
            evoxy_abi::HttpVersion::Http2 => Protocol::Http2,
        }
    }
}

/// The principal comes from Envoy-validated mTLS identity, not a self-parsed
/// certificate (docs/00 §5.4): first URI SAN, else subject DN, else anonymous.
fn principal_of(req: &FilterRequest) -> Principal {
    let id = if req.identity.presented && !req.identity.stable_id().is_empty() {
        req.identity.stable_id()
    } else {
        ANONYMOUS
    };
    Principal::new(PrincipalId::from(id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use evoxy_abi::{HttpVersion, MtlsIdentity};

    fn base(method: &str, path: &str) -> FilterRequest {
        FilterRequest {
            method: method.to_owned(),
            path_and_query: path.to_owned(),
            authority: "os.local".to_owned(),
            version: HttpVersion::Http2,
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            body: b"{}".to_vec(),
            identity: MtlsIdentity::default(),
        }
    }

    #[test]
    fn builds_ctx_that_matches_the_request() {
        let req = base("POST", "/orders/_search?routing=eu");
        let parts = RequestParts::from_filter(&req, "req-9").unwrap();
        let ctx = parts.ctx();
        assert_eq!(ctx.endpoint(), EndpointKind::Search);
        assert_eq!(ctx.logical_index(), "orders");
        assert_eq!(ctx.method(), HttpMethod::Post);
        assert_eq!(ctx.protocol(), Protocol::Http2);
        assert_eq!(ctx.query(), Some("routing=eu"));
        assert_eq!(ctx.path(), "/orders/_search");
        assert_eq!(ctx.request_id().as_str(), "req-9");
        assert_eq!(ctx.body(), b"{}");
    }

    #[test]
    fn principal_from_mtls_uri_san() {
        let mut req = base("GET", "/orders/_doc/1");
        req.identity = MtlsIdentity {
            presented: true,
            subject: "CN=ignored".to_owned(),
            uri_sans: vec!["spiffe://td/ingest".to_owned()],
        };
        let parts = RequestParts::from_filter(&req, "r").unwrap();
        assert_eq!(parts.ctx().principal_id().as_str(), "spiffe://td/ingest");
    }

    #[test]
    fn anonymous_when_no_cert_presented() {
        let req = base("GET", "/orders/_doc/1");
        let parts = RequestParts::from_filter(&req, "r").unwrap();
        assert_eq!(parts.ctx().principal_id().as_str(), ANONYMOUS);
    }

    #[test]
    fn grpc_content_type_overrides_version() {
        let mut req = base("POST", "/orders/_search");
        req.headers[0].1 = "application/grpc+proto".to_owned();
        let parts = RequestParts::from_filter(&req, "r").unwrap();
        assert_eq!(parts.ctx().protocol(), Protocol::Grpc);
    }

    #[test]
    fn unsupported_method_is_rejected() {
        let req = base("PATCH", "/orders/_doc/1");
        assert_eq!(
            RequestParts::from_filter(&req, "r").unwrap_err(),
            AdaptError::UnsupportedMethod("PATCH".to_owned())
        );
    }
}
