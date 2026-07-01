//! `EnvoyActions` implemented over an ext_proc `ProcessingResponse`.
//!
//! The brain issues effects; this records them as the header/body mutations (or
//! an immediate response) an ext_proc `CommonResponse` carries back to Envoy.
//!
//! Routing pseudo-headers (`:method`/`:path`) are emitted **only when they
//! actually change**: an unconditional overwrite of `:path` is a remove-then-add,
//! and Envoy applies the remove before the add, so re-emitting an unchanged path
//! transiently empties it and breaks route matching. We therefore compare against
//! the original request line and drop no-op routing mutations (`finish`).

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
/// routing/added headers, not append.
const OVERWRITE_IF_EXISTS_OR_ADD: i32 = 2;

/// Accumulates the brain's effects; the routing bits are resolved against the
/// original request line in [`ExtProcActions::finish`].
#[derive(Default)]
pub(crate) struct ExtProcActions {
    method: Option<String>,
    path: Option<String>,
    cluster: Option<String>,
    extra_headers: Vec<HeaderValueOption>,
    remove_headers: Vec<String>,
    body: Option<Vec<u8>>,
    immediate: Option<ImmediateResponse>,
}

fn overwrite(name: &str, value: &str) -> HeaderValueOption {
    HeaderValueOption {
        header: Some(HeaderValue {
            key: name.to_owned(),
            value: value.to_owned(),
            raw_value: Vec::new(),
        }),
        append_action: OVERWRITE_IF_EXISTS_OR_ADD,
        ..Default::default()
    }
}

impl ExtProcActions {
    /// Convert into the ext_proc `CommonResponse` mutations, or the immediate
    /// response if the brain sent a fail-closed reply. `orig_method`/`orig_path`
    /// are the request line as received, so an unchanged routing header is not
    /// re-emitted (which would transiently empty it).
    pub(crate) fn finish(
        self,
        orig_method: &str,
        orig_path: &str,
    ) -> Result<CommonResponse, ImmediateResponse> {
        if let Some(immediate) = self.immediate {
            return Err(immediate);
        }
        let mut set_headers = self.extra_headers;
        if let Some(method) = self.method.as_deref().filter(|m| *m != orig_method) {
            set_headers.push(overwrite(":method", method));
        }
        let path_changed = self.path.as_deref().is_some_and(|p| p != orig_path);
        if let Some(path) = self.path.as_deref().filter(|p| *p != orig_path) {
            set_headers.push(overwrite(":path", path));
        }
        if let Some(cluster) = self.cluster.as_deref() {
            set_headers.push(overwrite(CLUSTER_HEADER, cluster));
        }
        let header_mutation = if set_headers.is_empty() && self.remove_headers.is_empty() {
            None
        } else {
            Some(HeaderMutation {
                set_headers,
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
            // Re-run route selection when we changed where the request goes.
            clear_route_cache: self.cluster.is_some() || path_changed,
            ..Default::default()
        })
    }
}

impl EnvoyActions for ExtProcActions {
    fn set_upstream_cluster(&mut self, cluster: &str) {
        self.cluster = Some(cluster.to_owned());
    }
    fn set_method(&mut self, method: &str) {
        self.method = Some(method.to_owned());
    }
    fn set_path(&mut self, path: &str) {
        self.path = Some(path.to_owned());
    }
    fn set_body(&mut self, body: &[u8]) {
        self.body = Some(body.to_vec());
    }
    fn set_header(&mut self, name: &str, value: &str) {
        self.extra_headers.push(overwrite(name, value));
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
