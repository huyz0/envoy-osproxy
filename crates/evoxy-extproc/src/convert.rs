//! Assembling an [`evoxy_abi::FilterRequest`] from ext_proc messages.

use evoxy_abi::{FilterRequest, HttpVersion, MtlsIdentity};

use crate::extproc::HttpHeaders;

/// Flatten ext_proc's header map into `(name, value)` pairs. Envoy sends header
/// values in `raw_value` (bytes) or the legacy `value` (string); prefer the one
/// that is set.
pub(crate) fn extract_headers(headers: &HttpHeaders) -> Vec<(String, String)> {
    let Some(map) = headers.headers.as_ref() else {
        return Vec::new();
    };
    map.headers
        .iter()
        .map(|hv| {
            let value = if hv.value.is_empty() {
                String::from_utf8_lossy(&hv.raw_value).into_owned()
            } else {
                hv.value.clone()
            };
            (hv.key.clone(), value)
        })
        .collect()
}

/// Build a [`FilterRequest`] from the buffered headers and body. Envoy carries
/// the request line in HTTP/2 pseudo-headers (`:method`, `:path`, `:authority`),
/// and the Envoy-validated downstream identity in `x-forwarded-client-cert` (M4).
pub(crate) fn filter_request(headers: Vec<(String, String)>, body: Vec<u8>) -> FilterRequest {
    let get = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    };
    // Trust Envoy's XFCC (it terminated mTLS and set/sanitized the header); the
    // filter never parses a certificate itself.
    let identity = get("x-forwarded-client-cert")
        .map(|xfcc| MtlsIdentity::from_xfcc(&xfcc))
        .unwrap_or_default();
    FilterRequest {
        method: get(":method").unwrap_or_default(),
        path_and_query: get(":path").unwrap_or_default(),
        authority: get(":authority").unwrap_or_default(),
        version: HttpVersion::Http2,
        headers,
        body,
        identity,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_request_line_from_pseudo_headers() {
        let headers = vec![
            (":method".to_owned(), "PUT".to_owned()),
            (":path".to_owned(), "/orders/_doc/42".to_owned()),
            (":authority".to_owned(), "os.local".to_owned()),
        ];
        let req = filter_request(headers, Vec::new());
        assert_eq!(req.method, "PUT");
        assert_eq!(req.path(), "/orders/_doc/42");
        // No XFCC → no identity presented.
        assert!(!req.identity.presented);
    }

    #[test]
    fn populates_identity_from_xfcc() {
        let headers = vec![
            (":method".to_owned(), "GET".to_owned()),
            (
                "x-forwarded-client-cert".to_owned(),
                r#"Hash=abc;Subject="CN=svc";URI=spiffe://td/svc"#.to_owned(),
            ),
        ];
        let req = filter_request(headers, Vec::new());
        assert!(req.identity.presented);
        // The principal the brain keys on comes from Envoy-validated mTLS.
        assert_eq!(req.identity.stable_id(), "spiffe://td/svc");
    }
}
