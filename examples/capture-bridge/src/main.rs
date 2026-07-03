//! Example capture / async fan-out bridge (ADR-005).
//!
//! Envoy mirrors each request (already transformed by the evoxy filter) to this
//! service, which produces it to Kafka over [`evoxy_bridge::Bridge`]. There is no
//! tenancy code here: isolation already happened in the filter, so the record this
//! bridge produces is the physical, partition-scoped request. Point an Envoy route's
//! `request_mirror_policies` at this service (see
//! `examples/envoy/capture-fanout.yaml`).
//!
//! It uses an in-memory producer so it runs with no broker; swap in osproxy's real
//! `KrafkaProducer` (the `osproxy-kafka-rdkafka` crate) for production. Run it with:
//!
//! ```sh
//! cargo run -p capture-bridge        # listens on 0.0.0.0:8088
//! ```

use std::sync::Arc;

use evoxy_bridge::Bridge;
use osproxy_kafka::InMemoryProducer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // The Bridge is generic over the Producer seam; `InMemoryProducer` records
    // in memory. Replace it with `KrafkaProducer::new(...)` for a real broker.
    let bridge = Arc::new(Bridge::new(InMemoryProducer::new(), "evoxy.capture"));
    let addr = std::env::var("BRIDGE_ADDR").unwrap_or_else(|_| "0.0.0.0:8088".to_owned());
    let listener = TcpListener::bind(&addr).await?;
    eprintln!("capture bridge listening on {addr}, topic evoxy.capture");

    loop {
        let (sock, _) = listener.accept().await?;
        let bridge = bridge.clone();
        tokio::spawn(async move {
            if let Err(err) = handle(sock, &bridge).await {
                eprintln!("bridge connection error: {err}");
            }
        });
    }
}

/// Read one mirrored HTTP/1.1 request, produce it as a fan-out record, and answer
/// `200`. Envoy's mirror sends a buffered request with `content-length`, so this
/// reads the head, then the declared body length.
async fn handle(mut sock: TcpStream, bridge: &Bridge<InMemoryProducer>) -> std::io::Result<()> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(pos) = find(&buf, b"\r\n\r\n") {
            break pos;
        }
        let n = sock.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
    };

    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    // Request line: `PUT /orders_shared/_doc/acme%3A1 HTTP/1.1`.
    let path = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");
    let content_length = head
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);

    let mut body = buf[head_end + 4..].to_vec();
    while body.len() < content_length {
        let n = sock.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }

    // Produce the record (fire-and-forget, as the mirror is). A produce error is
    // logged, never surfaced to Envoy: the primary write already succeeded.
    if let Err(err) = bridge.forward(path, &body) {
        eprintln!("produce failed for {path}: {err:?}");
    } else {
        eprintln!("captured {path} ({} bytes)", body.len());
    }

    sock.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
        .await
}

/// The first index of `needle` in `hay`.
fn find(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len())
        .position(|window| window == needle)
}
