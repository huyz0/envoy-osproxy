//! The tonic `ExternalProcessor` service: a thin streaming shell over
//! [`process_message`](crate::process_message).

use std::sync::Arc;

use envoy_types::pb::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessor;
use evoxy_filter::Filter;
use osproxy_tenancy::Router;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::directives::Directives;
use crate::extproc::{ProcessingRequest, ProcessingResponse};
use crate::metrics::Metrics;
use crate::{process_message, AsyncWriteSink, StreamState, DEFAULT_MAX_REQUEST_BODY_BYTES};

/// The generated tonic server wrapper, re-exported so a binary can mount the
/// service: `Server::builder().add_service(ExternalProcessorServer::new(svc))`.
pub use envoy_types::pb::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessorServer;

/// The ext_proc service, generic over any tenancy [`Router`]. Each gRPC stream is
/// one downstream request's phases; state is per-stream.
pub struct ExtProcService<R> {
    filter: Arc<Filter<R>>,
    metrics: Arc<Metrics>,
    directives: Arc<Directives>,
    admin_token: Option<Arc<str>>,
    async_sink: Option<Arc<dyn AsyncWriteSink>>,
    max_request_body_bytes: usize,
}

// Hand-written so the bound is `Arc`-only, not `R: Clone`.
impl<R> Clone for ExtProcService<R> {
    fn clone(&self) -> Self {
        Self {
            filter: self.filter.clone(),
            metrics: self.metrics.clone(),
            directives: self.directives.clone(),
            admin_token: self.admin_token.clone(),
            async_sink: self.async_sink.clone(),
            max_request_body_bytes: self.max_request_body_bytes,
        }
    }
}

impl<R> std::fmt::Debug for ExtProcService<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ExtProcService")
    }
}

impl<R: Router> ExtProcService<R> {
    /// Build the service over a filter (the brain plus your tenancy), with the
    /// default request-body cap ([`DEFAULT_MAX_REQUEST_BODY_BYTES`]).
    #[must_use]
    pub fn new(filter: Filter<R>) -> Self {
        Self {
            filter: Arc::new(filter),
            metrics: Arc::new(Metrics::default()),
            directives: Arc::new(Directives::default()),
            admin_token: None,
            async_sink: None,
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
        }
    }

    /// Set the request-body cap: a body larger than this is refused with `413`
    /// before the brain runs, bounding the per-request working set.
    #[must_use]
    pub fn with_max_request_body_bytes(mut self, max_request_body_bytes: usize) -> Self {
        self.max_request_body_bytes = max_request_body_bytes;
        self
    }

    /// Enable the runtime directive plane (`/_evoxy/admin/directives`) behind this
    /// bearer token. Without it the plane fails closed `403` — off by default.
    #[must_use]
    pub fn with_admin_token(mut self, token: impl Into<Arc<str>>) -> Self {
        self.admin_token = Some(token.into());
        self
    }

    /// Enable async write mode (ADR-010): a write carrying
    /// `x-evoxy-write-mode: async` is produced durably to this sink and answered
    /// `202`, instead of forwarding to OpenSearch. Any [`Bridge`](evoxy_bridge::Bridge)
    /// over an [`AckProducer`](osproxy_kafka::AckProducer) is a sink. Without one the
    /// service refuses async requests with `503` (it never silently downgrades to a
    /// sync write). Sync requests are unaffected.
    #[must_use]
    pub fn with_async_write_sink(mut self, sink: Arc<dyn AsyncWriteSink>) -> Self {
        self.async_sink = Some(sink);
        self
    }
}

#[tonic::async_trait]
impl<R: Router> ExternalProcessor for ExtProcService<R> {
    type ProcessStream = ReceiverStream<Result<ProcessingResponse, Status>>;

    async fn process(
        &self,
        request: Request<Streaming<ProcessingRequest>>,
    ) -> Result<Response<Self::ProcessStream>, Status> {
        let mut inbound = request.into_inner();
        let filter = self.filter.clone();
        let metrics = self.metrics.clone();
        let directives = self.directives.clone();
        let admin_token = self.admin_token.clone();
        let async_sink = self.async_sink.clone();
        let max_request_body_bytes = self.max_request_body_bytes;
        let (tx, rx) = mpsc::channel(16);

        // One task per stream reads request phases and streams back responses.
        // A gRPC service legitimately spawns (the transport owns the runtime).
        tokio::spawn(async move {
            let mut state = StreamState::new(max_request_body_bytes);
            loop {
                match inbound.message().await {
                    Ok(Some(req)) => {
                        let resp = process_message(
                            &filter,
                            &metrics,
                            &directives,
                            admin_token.as_deref(),
                            async_sink.as_deref(),
                            &mut state,
                            req,
                        )
                        .await;
                        if tx.send(Ok(resp)).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
