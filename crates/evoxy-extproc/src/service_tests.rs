//! Live-loop test for the tonic service shell: a real gRPC round-trip over a
//! loopback socket (no Docker, no Envoy), proving the per-stream task reads request
//! phases and streams back responses — the part otherwise covered only by the
//! Docker-gated e2e.

use envoy_types::pb::envoy::config::core::v3::{HeaderMap, HeaderValue};
use envoy_types::pb::envoy::service::ext_proc::v3::external_processor_client::ExternalProcessorClient;
use evoxy_filter::{Filter, ReferenceTenancy};
use osproxy_tenancy::TenancyRouter;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::StreamExt;

use crate::extproc::processing_request::Request as Req;
use crate::extproc::processing_response::Response as Resp;
use crate::extproc::{HttpBody, HttpHeaders, ProcessingRequest};
use crate::service::{ExtProcService, ExternalProcessorServer};

/// Serve the reference-tenancy service on an ephemeral loopback port; returns a
/// connected client. The server task lives as long as the test.
async fn serve() -> ExternalProcessorClient<tonic::transport::Channel> {
    let filter = Filter::new(TenancyRouter::new(ReferenceTenancy::new(
        "opensearch",
        "http://os:9200",
        "x-tenant",
    )));
    let service = ExtProcService::new(filter)
        .with_max_request_body_bytes(1024)
        .with_admin_token("s3cret");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(ExternalProcessorServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    ExternalProcessorClient::connect(format!("http://{addr}"))
        .await
        .expect("connect to the in-process service")
}

fn headers_msg(pairs: &[(&str, &str)], end_of_stream: bool) -> ProcessingRequest {
    let headers = pairs
        .iter()
        .map(|(k, v)| HeaderValue {
            key: (*k).to_owned(),
            value: (*v).to_owned(),
            raw_value: Vec::new(),
        })
        .collect();
    ProcessingRequest {
        request: Some(Req::RequestHeaders(HttpHeaders {
            headers: Some(HeaderMap { headers }),
            end_of_stream,
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn body_msg(body: &[u8]) -> ProcessingRequest {
    ProcessingRequest {
        request: Some(Req::RequestBody(HttpBody {
            body: body.to_vec(),
            end_of_stream: true,
            ..Default::default()
        })),
        ..Default::default()
    }
}

/// One full write over real gRPC: headers phase continues, body phase returns the
/// routing mutation, and the stream closes cleanly when the client is done.
#[tokio::test]
async fn grpc_stream_routes_a_write_end_to_end() {
    let mut client = serve().await;

    let outbound = tokio_stream::iter(vec![
        headers_msg(
            &[
                (":method", "PUT"),
                (":path", "/orders/_doc/42"),
                ("x-tenant", "acme"),
            ],
            false,
        ),
        body_msg(br#"{"k":1}"#),
    ]);
    let mut inbound = client
        .process(tonic::Request::new(outbound))
        .await
        .expect("open the stream")
        .into_inner();

    // Headers phase: continue, no mutation yet.
    let first = inbound
        .next()
        .await
        .expect("a headers-phase response")
        .expect("no status error");
    assert!(matches!(first.response, Some(Resp::RequestHeaders(_))));

    // Body phase: the routing mutation carries the cluster header.
    let second = inbound
        .next()
        .await
        .expect("a body-phase response")
        .expect("no status error");
    let common = match second.response {
        Some(Resp::RequestBody(b)) => b.response,
        _ => None,
    }
    .expect("a RequestBody mutation");
    let cluster = common
        .header_mutation
        .as_ref()
        .and_then(|m| {
            m.set_headers
                .iter()
                .filter_map(|o| o.header.as_ref())
                .find(|h| h.key == "x-evoxy-cluster")
        })
        .expect("the cluster routing header");
    assert_eq!(String::from_utf8_lossy(&cluster.raw_value), "opensearch");

    // Client closes the outbound stream: the server side ends without an error.
    assert!(inbound.next().await.is_none());
}

/// The reserved metrics path is answered over the same gRPC stream as an
/// immediate response (the introspection surface rides the data plane).
#[tokio::test]
async fn grpc_stream_answers_the_metrics_path() {
    let mut client = serve().await;
    let outbound = tokio_stream::iter(vec![headers_msg(
        &[(":method", "GET"), (":path", "/_evoxy/metrics")],
        true,
    )]);
    let mut inbound = client
        .process(tonic::Request::new(outbound))
        .await
        .expect("open the stream")
        .into_inner();

    let reply = inbound
        .next()
        .await
        .expect("a metrics response")
        .expect("no status error");
    let immediate = match reply.response {
        Some(Resp::ImmediateResponse(imm)) => Some(imm),
        _ => None,
    }
    .expect("an immediate metrics reply");
    assert_eq!(immediate.status.map(|s| s.code), Some(200));
    assert!(String::from_utf8_lossy(&immediate.body).contains("\"requests\""));
}
