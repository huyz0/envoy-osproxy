//! Percent-encoding for the doc-id path segment and `_routing` query value.
//!
//! A physical doc id can contain reserved characters — most importantly `/`, when
//! a partition is a URI (a SPIFFE principal) and the id template embeds it
//! (`{partition}:{body.id}` → `spiffe://td/acme:1`). Left raw in the forwarded
//! `:path`, the slashes split the path into extra segments and OpenSearch answers
//! `no handler found`. We percent-encode the id segment (and the `_routing` value)
//! so the wire carries a single segment; OpenSearch decodes it back to the exact
//! id, so the **stored** id and every response are byte-for-byte unchanged — the
//! encoding is invisible past the URL.

const HEX: &[u8; 16] = b"0123456789ABCDEF";

/// Percent-encode `s` as one URL path segment / query value: every byte outside
/// the RFC 3986 *unreserved* set (`ALPHA` / `DIGIT` / `-` `.` `_` `~`) is escaped.
/// Encoding more than strictly necessary is safe — the server decodes it either
/// way — so we escape all reserved bytes rather than reason about which are
/// tolerated in which position.
pub(crate) fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(b as char);
        } else {
            out.push('%');
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unreserved_pass_through() {
        assert_eq!(encode("abcXYZ0189-._~"), "abcXYZ0189-._~");
    }

    #[test]
    fn slashes_and_colons_are_escaped() {
        // The case that motivated this: a SPIFFE-derived physical id.
        assert_eq!(encode("spiffe://td/acme:1"), "spiffe%3A%2F%2Ftd%2Facme%3A1");
    }

    #[test]
    fn other_reserved_and_utf8_are_escaped() {
        assert_eq!(encode("a b?c#d%e"), "a%20b%3Fc%23d%25e");
        // A multi-byte char is escaped byte-by-byte (é = 0xC3 0xA9).
        assert_eq!(encode("é"), "%C3%A9");
    }
}
