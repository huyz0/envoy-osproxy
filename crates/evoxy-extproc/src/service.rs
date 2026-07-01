//! The tonic `ExternalProcessor` service: a thin streaming shell over
//! [`process_message`](crate::process_message).

use std::sync::Arc;

use envoy_types::pb::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessor;
use evoxy_filter::{Filter, ReferenceTenancy};
use osproxy_tenancy::TenancyRouter;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::extproc::{ProcessingRequest, ProcessingResponse};
use crate::{process_message, StreamState};

/// The generated tonic server wrapper, re-exported so a binary can mount the
/// service: `Server::builder().add_service(ExternalProcessorServer::new(svc))`.
pub use envoy_types::pb::envoy::service::ext_proc::v3::external_processor_server::ExternalProcessorServer;

/// The router the ext_proc service is built over.
///
/// Concrete (the reference tenancy) rather than generic over `Router`: a generic
/// service cannot spawn the response task, because `Router::resolve` is an
/// `async fn` in a trait and its future is not provably `Send` for a generic
/// parameter. A concrete router makes the future `Send`, which the streamed task
/// requires. A user-tenancy service is the same shape monomorphized over that
/// tenancy (a small macro at the binary — deferred; see the roadmap).
type ServiceRouter = TenancyRouter<ReferenceTenancy>;

/// The ext_proc service. Each gRPC stream is one downstream request's phases;
/// state is per-stream.
#[derive(Clone)]
pub struct ExtProcService {
    filter: Arc<Filter<ServiceRouter>>,
}

impl std::fmt::Debug for ExtProcService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ExtProcService")
    }
}

impl ExtProcService {
    /// Build the service over a filter (the brain + the reference tenancy).
    #[must_use]
    pub fn new(filter: Filter<ServiceRouter>) -> Self {
        Self {
            filter: Arc::new(filter),
        }
    }
}

#[tonic::async_trait]
impl ExternalProcessor for ExtProcService {
    type ProcessStream = ReceiverStream<Result<ProcessingResponse, Status>>;

    async fn process(
        &self,
        request: Request<Streaming<ProcessingRequest>>,
    ) -> Result<Response<Self::ProcessStream>, Status> {
        let mut inbound = request.into_inner();
        let filter = self.filter.clone();
        let (tx, rx) = mpsc::channel(16);

        // One task per stream reads request phases and streams back responses.
        // A gRPC service legitimately spawns (the transport owns the runtime).
        tokio::spawn(async move {
            let mut state = StreamState::default();
            loop {
                match inbound.message().await {
                    Ok(Some(req)) => {
                        let resp = process_message(&filter, &mut state, req).await;
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
