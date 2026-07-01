//! Path/method → endpoint classification.
//!
//! The Envoy-side analog of osproxy's `transport::classify`: Envoy hands us a raw
//! `:path`, so the adapter is where a request is classed into an
//! [`EndpointKind`] and its logical index and doc id are extracted. Kept
//! deliberately pure (no allocation beyond the owned strings the borrowing
//! `RequestCtx` needs) so it is cheap on the hot path (docs/09).

use osproxy_core::EndpointKind;
use osproxy_spi::HttpMethod;

/// The routing-relevant facets parsed out of the request line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classified {
    /// The logical index the client addressed, empty for cluster-level endpoints
    /// (`/_bulk`, `/_msearch`, …) that carry the index per action instead.
    pub logical_index: String,
    /// The endpoint class that selects the pipeline behavior.
    pub endpoint: EndpointKind,
    /// The client-supplied document id, for by-id endpoints.
    pub doc_id: Option<String>,
}

/// Classify a request from its method and path. Never fails: an unrecognized
/// shape is [`EndpointKind::Unknown`], which the pipeline rejects (or passes
/// through when configured) — fail-closed by default, exactly as osproxy does.
#[must_use]
pub fn classify(method: HttpMethod, path: &str) -> Classified {
    let segments: Vec<&str> = path
        .trim_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();
    match segments.as_slice() {
        // Root / health.
        [] => unknown(""),

        // Cluster-level endpoints (no index in the path).
        ["_bulk"] => at("", EndpointKind::IngestBulk, None),
        ["_mget"] => at("", EndpointKind::MultiGet, None),
        ["_msearch"] => at("", EndpointKind::MultiSearch, None),
        ["_search"] => at("", EndpointKind::Search, None),
        ["_count"] => at("", EndpointKind::Count, None),
        ["_search", "scroll", ..] => at("", EndpointKind::Cursor, None),
        [first, ..] if is_admin_root(first) => at("", EndpointKind::Admin, None),

        // Index-scoped endpoints.
        [index, verb, rest @ ..] => index_scoped(method, index, verb, rest),

        // A bare `/{index}` (index create/exists/settings) — admin-ish; M0 treats
        // it as Unknown (no tenancy semantics wired yet).
        [index] => unknown(index),
    }
}

fn index_scoped(method: HttpMethod, index: &str, verb: &str, rest: &[&str]) -> Classified {
    let doc_id = rest.first().map(|s| (*s).to_owned());
    match verb {
        "_doc" | "_create" | "_update" => at(index, doc_endpoint(method, verb), doc_id),
        "_bulk" => at(index, EndpointKind::IngestBulk, None),
        "_search" => at(index, EndpointKind::Search, None),
        "_count" => at(index, EndpointKind::Count, None),
        "_mget" => at(index, EndpointKind::MultiGet, None),
        "_msearch" => at(index, EndpointKind::MultiSearch, None),
        "_delete_by_query" => at(index, EndpointKind::DeleteByQuery, None),
        "_pit" => at(index, EndpointKind::Cursor, None),
        _ => unknown(index),
    }
}

/// `_doc`/`_create`/`_update` are method-polymorphic: a `GET`/`DELETE` on
/// `_doc/{id}` is a read/delete-by-id; `PUT`/`POST` is an ingest. `_create` and
/// `_update` are always ingest.
fn doc_endpoint(method: HttpMethod, verb: &str) -> EndpointKind {
    match (verb, method) {
        ("_doc", HttpMethod::Get | HttpMethod::Head) => EndpointKind::GetById,
        ("_doc", HttpMethod::Delete) => EndpointKind::DeleteById,
        _ => EndpointKind::IngestDoc,
    }
}

fn is_admin_root(seg: &str) -> bool {
    matches!(
        seg,
        "_cat" | "_cluster" | "_nodes" | "_aliases" | "_tasks" | "_snapshot"
    )
}

fn at(index: &str, endpoint: EndpointKind, doc_id: Option<String>) -> Classified {
    Classified {
        logical_index: index.to_owned(),
        endpoint,
        doc_id,
    }
}

fn unknown(index: &str) -> Classified {
    at(index, EndpointKind::Unknown, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_by_id_is_put() {
        let c = classify(HttpMethod::Put, "/orders/_doc/42");
        assert_eq!(c.logical_index, "orders");
        assert_eq!(c.endpoint, EndpointKind::IngestDoc);
        assert_eq!(c.doc_id.as_deref(), Some("42"));
    }

    #[test]
    fn get_by_id_vs_delete_by_id() {
        assert_eq!(
            classify(HttpMethod::Get, "/orders/_doc/42").endpoint,
            EndpointKind::GetById
        );
        assert_eq!(
            classify(HttpMethod::Delete, "/orders/_doc/42").endpoint,
            EndpointKind::DeleteById
        );
    }

    #[test]
    fn create_and_update_are_ingest_regardless_of_method() {
        assert_eq!(
            classify(HttpMethod::Put, "/o/_create/1").endpoint,
            EndpointKind::IngestDoc
        );
        assert_eq!(
            classify(HttpMethod::Post, "/o/_update/1").endpoint,
            EndpointKind::IngestDoc
        );
    }

    #[test]
    fn index_scoped_reads_and_bulk() {
        assert_eq!(
            classify(HttpMethod::Post, "/orders/_search").endpoint,
            EndpointKind::Search
        );
        assert_eq!(
            classify(HttpMethod::Get, "/orders/_count").endpoint,
            EndpointKind::Count
        );
        assert_eq!(
            classify(HttpMethod::Post, "/orders/_bulk").endpoint,
            EndpointKind::IngestBulk
        );
        assert_eq!(
            classify(HttpMethod::Post, "/orders/_mget").endpoint,
            EndpointKind::MultiGet
        );
        assert_eq!(
            classify(HttpMethod::Post, "/orders/_msearch").endpoint,
            EndpointKind::MultiSearch
        );
    }

    #[test]
    fn cluster_level_endpoints_have_no_index() {
        let c = classify(HttpMethod::Post, "/_bulk");
        assert_eq!(c.logical_index, "");
        assert_eq!(c.endpoint, EndpointKind::IngestBulk);
        assert_eq!(
            classify(HttpMethod::Post, "/_msearch").endpoint,
            EndpointKind::MultiSearch
        );
    }

    #[test]
    fn scroll_and_pit_are_cursor() {
        assert_eq!(
            classify(HttpMethod::Get, "/_search/scroll").endpoint,
            EndpointKind::Cursor
        );
        assert_eq!(
            classify(HttpMethod::Post, "/orders/_pit").endpoint,
            EndpointKind::Cursor
        );
    }

    #[test]
    fn admin_and_unknown() {
        assert_eq!(
            classify(HttpMethod::Get, "/_cat/indices").endpoint,
            EndpointKind::Admin
        );
        assert_eq!(
            classify(HttpMethod::Get, "/orders").endpoint,
            EndpointKind::Unknown
        );
        assert_eq!(
            classify(HttpMethod::Get, "/").endpoint,
            EndpointKind::Unknown
        );
    }

    #[test]
    fn delete_by_query_classifies() {
        assert_eq!(
            classify(HttpMethod::Post, "/orders/_delete_by_query").endpoint,
            EndpointKind::DeleteByQuery
        );
    }
}
