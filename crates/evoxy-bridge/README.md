# evoxy-bridge

The async fan-out sink. An Envoy extension cannot cleanly produce to Kafka from the
request path, so fan-out uses Envoy's request-mirror to shadow a copy of the
transformed request to a small HTTP service. This crate is that service's core: it
turns a mirrored request into a Kafka record.

`Bridge` wraps a producer and a topic. `forward` sends a record fire-and-forget;
`forward_acked` waits for the broker acknowledgement when the caller needs
at-least-once. The producer is a trait from osproxy's Kafka seam, so osproxy's real
Kafka producer plugs straight in, and the tests use an in-memory one. The crate
itself pulls in no broker client and no crypto, so it stays light.
