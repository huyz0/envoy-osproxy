//! `EnvoyActions` implemented over an ext_proc `ProcessingResponse`.
//!
//! The brain issues effects; this records them as the header/body mutations (or
//! an immediate response) an ext_proc `CommonResponse` carries back to Envoy.

use envoy_types::pb::envoy::config::core::v3::{HeaderValue, HeaderValueOption};
use envoy_types::pb::envoy::r#type::v3::HttpStatus;
use evoxy_filter::EnvoyActions;

use crate::extproc::{
    body_mutation, BodyMutation, CommonResponse, HeaderMutation, ImmediateResponse,
};

/// The request header cluster selection sets; the Envoy route config matches on
/// it to choose the upstream cluster (the ADR-002 `Target → cluster` seam).
pub const CLUSTER_HEADER: &str = "x-evoxy-cluster";

/// `HeaderValueOption.append_action` = `OVERWRITE_IF_EXISTS_OR_ADD` — we replace
/// `:method`/`:path`/routing headers, not append.
const OVERWRITE_IF_EXISTS_OR_ADD: i32 = 2;

/// Accumulates the brain's effects into ext_proc mutation form.
#[derive(Default)]
pub(crate) struct ExtProcActions {
    set_headers: Vec<HeaderValueOption>,
    remove_headers: Vec<String>,
    body: Option<Vec<u8>>,
    clear_route_cache: bool,
    immediate: Option<ImmediateResponse>,
}

impl ExtProcActions {
    fn put(&mut self, name: &str, value: &str) {
        self.set_headers.push(HeaderValueOption {
            header: Some(HeaderValue {
                key: name.to_owned(),
                value: value.to_owned(),
                raw_value: Vec::new(),
            }),
            append_action: OVERWRITE_IF_EXISTS_OR_ADD,
            ..Default::default()
        });
    }

    /// Convert into the ext_proc `CommonResponse` mutations, or the immediate
    /// response if the brain sent a fail-closed reply.
    pub(crate) fn finish(self) -> Result<CommonResponse, ImmediateResponse> {
        if let Some(immediate) = self.immediate {
            return Err(immediate);
        }
        let header_mutation = if self.set_headers.is_empty() && self.remove_headers.is_empty() {
            None
        } else {
            Some(HeaderMutation {
                set_headers: self.set_headers,
                remove_headers: self.remove_headers,
            })
        };
        let body_mutation = self.body.map(|body| BodyMutation {
            mutation: Some(body_mutation::Mutation::Body(body)),
        });
        Ok(CommonResponse {
            status: 0, // CONTINUE
            header_mutation,
            body_mutation,
            clear_route_cache: self.clear_route_cache,
            ..Default::default()
        })
    }
}

impl EnvoyActions for ExtProcActions {
    fn set_upstream_cluster(&mut self, cluster: &str) {
        self.put(CLUSTER_HEADER, cluster);
        // The route was chosen from the original path; force a re-match so the
        // cluster header takes effect.
        self.clear_route_cache = true;
    }
    fn set_method(&mut self, method: &str) {
        self.put(":method", method);
    }
    fn set_path(&mut self, path: &str) {
        self.put(":path", path);
    }
    fn set_body(&mut self, body: &[u8]) {
        self.body = Some(body.to_vec());
    }
    fn set_header(&mut self, name: &str, value: &str) {
        self.put(name, value);
    }
    fn remove_header(&mut self, name: &str) {
        self.remove_headers.push(name.to_owned());
    }
    fn send_local_reply(&mut self, status: u16, _headers: &[(String, String)], body: &[u8]) {
        self.immediate = Some(ImmediateResponse {
            status: Some(HttpStatus {
                code: i32::from(status),
            }),
            body: body.to_vec(),
            ..Default::default()
        });
    }
}
