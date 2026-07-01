//! Parsing Envoy's `x-forwarded-client-cert` (XFCC) header into an
//! [`MtlsIdentity`](crate::MtlsIdentity) (docs/00 §5.4, M4).
//!
//! When Envoy terminates mTLS it forwards the validated client identity in the
//! XFCC header, so the filter trusts this instead of parsing a certificate
//! itself. The header is a comma-separated list of elements (one per cert in the
//! chain, the client's own cert first), each a `;`-separated list of `Key=Value`
//! pairs — e.g.
//!
//! ```text
//! By=spiffe://td/ingress;Hash=abc;Subject="CN=svc,O=org";URI=spiffe://td/svc
//! ```
//!
//! We read only the **first** element (the peer certificate) and take its
//! `Subject` and any `URI` SANs. Values may be double-quoted (a `Subject` DN
//! usually is, since it contains commas), so the split is quote-aware. This trusts
//! Envoy to have set the header and stripped any client-supplied one
//! (`forward_client_cert_details: SANITIZE_SET`); the filter never sees a raw
//! certificate.

use crate::MtlsIdentity;

impl MtlsIdentity {
    /// Parse an XFCC header value into an identity. An empty (or whitespace-only)
    /// header yields the default identity (`presented == false`).
    #[must_use]
    pub fn from_xfcc(header: &str) -> Self {
        if header.trim().is_empty() {
            return Self::default();
        }
        // Only the first element — the peer's own certificate — is the identity we
        // key tenancy on; the rest are the issuing chain.
        let peer = split_top_level(header, ',').next().unwrap_or("").trim();
        let mut identity = Self {
            presented: true,
            subject: String::new(),
            uri_sans: Vec::new(),
        };
        for pair in split_top_level(peer, ';') {
            let Some((key, value)) = pair.trim().split_once('=') else {
                continue;
            };
            let value = unquote(value.trim());
            if key.eq_ignore_ascii_case("Subject") {
                identity.subject = value;
            } else if key.eq_ignore_ascii_case("URI") {
                // URI can repeat; keep presentation order, skip empties.
                if !value.is_empty() {
                    identity.uri_sans.push(value);
                }
            }
        }
        identity
    }
}

/// Split `input` on `delim`, but not inside a double-quoted span (`\"` escapes a
/// quote). Returns borrowed slices with quotes preserved; unquoting is separate.
fn split_top_level(input: &str, delim: char) -> impl Iterator<Item = &str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_quotes = false;
    let mut escaped = false;
    for (idx, ch) in input.char_indices() {
        if escaped {
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            in_quotes = !in_quotes;
        } else if ch == delim && !in_quotes {
            parts.push(&input[start..idx]);
            start = idx + ch.len_utf8();
        }
    }
    parts.push(&input[start..]);
    parts.into_iter()
}

/// Strip surrounding double quotes and unescape `\"`/`\\` in a quoted value; a
/// bare value is returned as-is.
fn unquote(value: &str) -> String {
    let inner = value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix('"'))
        .unwrap_or(value);
    let mut out = String::with_capacity(inner.len());
    let mut escaped = false;
    for ch in inner.chars() {
        if escaped {
            out.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_header_is_not_presented() {
        assert_eq!(MtlsIdentity::from_xfcc(""), MtlsIdentity::default());
        assert_eq!(MtlsIdentity::from_xfcc("   "), MtlsIdentity::default());
    }

    #[test]
    fn parses_uri_san_and_subject() {
        let id = MtlsIdentity::from_xfcc(
            r#"By=spiffe://td/ingress;Hash=abc123;Subject="CN=svc,O=org";URI=spiffe://td/svc"#,
        );
        assert!(id.presented);
        assert_eq!(id.subject, "CN=svc,O=org");
        assert_eq!(id.uri_sans, vec!["spiffe://td/svc".to_owned()]);
        // The SPIFFE URI SAN is the stable principal.
        assert_eq!(id.stable_id(), "spiffe://td/svc");
    }

    #[test]
    fn quoted_subject_with_commas_is_not_split() {
        let id = MtlsIdentity::from_xfcc(r#"Subject="CN=a,OU=b,O=c";Hash=x"#);
        assert_eq!(id.subject, "CN=a,OU=b,O=c");
    }

    #[test]
    fn reads_only_the_peer_cert_from_a_chain() {
        // Two elements (peer, issuer); only the peer's identity is taken.
        let id = MtlsIdentity::from_xfcc(
            r#"Subject="CN=peer";URI=spiffe://td/peer,Subject="CN=issuer";URI=spiffe://td/issuer"#,
        );
        assert_eq!(id.subject, "CN=peer");
        assert_eq!(id.uri_sans, vec!["spiffe://td/peer".to_owned()]);
    }

    #[test]
    fn subject_only_falls_back_to_dn_principal() {
        let id = MtlsIdentity::from_xfcc(r#"Hash=abc;Subject="CN=svc""#);
        assert!(id.presented);
        assert!(id.uri_sans.is_empty());
        assert_eq!(id.stable_id(), "CN=svc");
    }

    #[test]
    fn unknown_keys_and_missing_equals_are_skipped() {
        let id = MtlsIdentity::from_xfcc("Hash=abc;garbage;DNS=svc.local;URI=spiffe://td/svc");
        assert_eq!(id.uri_sans, vec!["spiffe://td/svc".to_owned()]);
        assert!(id.subject.is_empty());
    }
}
