//! Tests for the shared async-write contract: `Filter::async_write` against fake
//! acknowledging / failing sinks.

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use evoxy_abi::{FilterRequest, HttpVersion, MtlsIdentity};
use osproxy_tenancy::TenancyRouter;

use super::*;
use crate::{Filter, FilterConfig, ReferenceTenancy};

/// A recording sink: captures the `(path, body)` of each acknowledged produce, as a
/// broker that accepted the record would.
#[derive(Default)]
struct RecordingSink {
    acked: Mutex<Vec<(String, Vec<u8>)>>,
}

impl AsyncWriteSink for RecordingSink {
    fn produce_acked<'a>(
        &'a self,
        path: &'a str,
        body: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async move {
            self.acked
                .lock()
                .unwrap()
                .push((path.to_owned(), body.to_vec()));
            Ok(())
        })
    }
}

/// A sink whose produce is never acknowledged (the broker is down).
struct FailingSink;

impl AsyncWriteSink for FailingSink {
    fn produce_acked<'a>(
        &'a self,
        _path: &'a str,
        _body: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), ProduceError>> + Send + 'a>> {
        Box::pin(async {
            Err(ProduceError {
                reason: "broker unavailable",
            })
        })
    }
}

/// A dedicated-index reference filter: the transform rewrites the path to a
/// per-tenant physical index, so the produced record visibly differs from the raw
/// request.
fn dedicated_index_filter() -> Filter<TenancyRouter<ReferenceTenancy>> {
    crate::reference_filter(&FilterConfig::from_json(
        r#"{"isolation":"dedicated_index","cluster":"opensearch","index_template":"orders-{partition}","partition_header":"x-tenant"}"#,
    ))
}

fn write_req(path: &str, tenant: Option<&str>, body: &[u8]) -> FilterRequest {
    let mut headers = vec![
        (":method".to_owned(), "PUT".to_owned()),
        (":path".to_owned(), path.to_owned()),
    ];
    if let Some(t) = tenant {
        headers.push(("x-tenant".to_owned(), t.to_owned()));
    }
    FilterRequest {
        method: "PUT".to_owned(),
        path_and_query: path.to_owned(),
        authority: String::new(),
        version: HttpVersion::Http2,
        headers,
        body: body.to_vec(),
        identity: MtlsIdentity::default(),
    }
}

#[tokio::test]
async fn produces_the_transformed_request_and_returns_202() {
    let filter = dedicated_index_filter();
    let sink = RecordingSink::default();
    let reply = filter
        .async_write(
            &write_req("/orders/_doc/42", Some("acme"), br#"{"k":1}"#),
            Some(&sink),
        )
        .await;

    assert_eq!(reply.status, 202);
    let body = String::from_utf8(reply.body).unwrap();
    assert!(body.contains("\"status\":\"accepted\""), "202 body: {body}");
    assert!(body.contains("\"op_id\":\""), "carries an op_id: {body}");

    let acked = sink.acked.lock().unwrap();
    assert_eq!(acked.len(), 1);
    // The physical request, not the raw client path — isolation applied first.
    assert!(
        acked[0].0.contains("orders-acme"),
        "produced physical path: {}",
        acked[0].0
    );
}

#[tokio::test]
async fn refuses_503_when_broker_does_not_ack() {
    let filter = dedicated_index_filter();
    let reply = filter
        .async_write(
            &write_req("/orders/_doc/42", Some("acme"), br#"{"k":1}"#),
            Some(&FailingSink),
        )
        .await;
    assert_eq!(reply.status, 503);
    assert_eq!(
        String::from_utf8(reply.body).unwrap(),
        r#"{"error":"fanout_unavailable"}"#
    );
}

#[tokio::test]
async fn refuses_503_when_no_sink() {
    let filter = dedicated_index_filter();
    let reply = filter
        .async_write(
            &write_req("/orders/_doc/42", Some("acme"), br#"{"k":1}"#),
            None,
        )
        .await;
    assert_eq!(reply.status, 503);
    assert_eq!(
        String::from_utf8(reply.body).unwrap(),
        r#"{"error":"async_write_unavailable"}"#
    );
}

#[tokio::test]
async fn rejects_400_on_a_read() {
    let filter = dedicated_index_filter();
    let sink = RecordingSink::default();
    let mut req = write_req("/orders/_doc/42", Some("acme"), &[]);
    req.method = "GET".to_owned();
    for h in &mut req.headers {
        if h.0 == ":method" {
            h.1 = "GET".to_owned();
        }
    }
    let reply = filter.async_write(&req, Some(&sink)).await;
    assert_eq!(reply.status, 400);
    assert!(sink.acked.lock().unwrap().is_empty(), "nothing produced");
}

#[tokio::test]
async fn a_fail_closed_transform_is_surfaced_not_accepted() {
    // No tenant header → the transform fails closed 400; it never becomes a 202.
    let filter = dedicated_index_filter();
    let sink = RecordingSink::default();
    let reply = filter
        .async_write(
            &write_req("/orders/_doc/42", None, br#"{"k":1}"#),
            Some(&sink),
        )
        .await;
    assert_eq!(reply.status, 400);
    assert!(
        sink.acked.lock().unwrap().is_empty(),
        "no produce on reject"
    );
}

#[test]
fn wants_async_matches_the_header() {
    assert!(wants_async(&[(
        "x-evoxy-write-mode".to_owned(),
        "async".to_owned()
    )]));
    assert!(wants_async(&[(
        "X-Evoxy-Write-Mode".to_owned(),
        "ASYNC".to_owned()
    )]));
    assert!(!wants_async(&[(
        "x-evoxy-write-mode".to_owned(),
        "sync".to_owned()
    )]));
    assert!(!wants_async(&[]));
}

/// The produced record's key is the brain's physical path, query included: a
/// shared-index write carries its constructed `?routing=` on the key.
#[tokio::test]
async fn produced_key_is_the_physical_path_with_query() {
    let filter = crate::reference_filter(&FilterConfig::from_json(
        r#"{"shared_index":"orders_shared","inject_field":"_t","id_template":"{partition}:{body.id}","partition_header":"x-tenant"}"#,
    ));
    let sink = RecordingSink::default();
    let reply = filter
        .async_write(
            &write_req("/orders/_doc/7", Some("acme"), br#"{"id":7}"#),
            Some(&sink),
        )
        .await;
    assert_eq!(reply.status, 202);
    let acked = sink.acked.lock().unwrap();
    // Physical index, partition-scoped id, and the constructed routing query.
    assert_eq!(acked[0].0, "/orders_shared/_doc/acme%3A7?routing=acme");
    // The injected isolation field rides the payload.
    assert!(String::from_utf8_lossy(&acked[0].1).contains(r#""_t":"acme""#));
}
