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
/// the request line in HTTP/2 pseudo-headers (`:method`, `:path`, `:authority`).
/// mTLS identity would come from `x-forwarded-client-cert` (wired at M4);
/// defaulted for now.
pub(crate) fn filter_request(headers: Vec<(String, String)>, body: Vec<u8>) -> FilterRequest {
    let get = |name: &str| {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    };
    FilterRequest {
        method: get(":method").unwrap_or_default(),
        path_and_query: get(":path").unwrap_or_default(),
        authority: get(":authority").unwrap_or_default(),
        version: HttpVersion::Http2,
        headers,
        body,
        identity: MtlsIdentity::default(),
    }
}
