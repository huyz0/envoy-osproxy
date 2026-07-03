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
    host: Option<String>,
    extra_headers: Vec<HeaderValueOption>,
    remove_headers: Vec<String>,
    body: Option<Vec<u8>>,
    immediate: Option<ImmediateResponse>,
}

fn overwrite(name: &str, value: &str) -> HeaderValueOption {
    HeaderValueOption {
        header: Some(HeaderValue {
            key: name.to_owned(),
            // Envoy applies the byte `raw_value`, not the deprecated string
            // `value`; setting only `value` leaves the header empty (an empty
            // `:path` becomes a malformed upstream request).
            value: String::new(),
            raw_value: value.as_bytes().to_vec(),
        }),
        append_action: OVERWRITE_IF_EXISTS_OR_ADD,
        ..Default::default()
    }
}

impl ExtProcActions {
    /// The physical request the brain transformed `(method, path, body)`, for the
    /// async-write path to produce instead of forwarding. A field the brain left
    /// unchanged falls back to the original request line / an empty body. `Err` is
    /// the fail-closed immediate reply the brain sent (an unresolved/rejected
    /// request never becomes an accepted async write).
    pub(crate) fn transformed(
        self,
        orig_method: &str,
        orig_path: &str,
    ) -> Result<(String, String, Vec<u8>), ImmediateResponse> {
        if let Some(immediate) = self.immediate {
            return Err(immediate);
        }
        let method = self.method.unwrap_or_else(|| orig_method.to_owned());
        let path = self.path.unwrap_or_else(|| orig_path.to_owned());
        Ok((method, path, self.body.unwrap_or_default()))
    }

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
        let mut remove_headers = self.remove_headers;
        if let Some(method) = self.method.as_deref().filter(|m| *m != orig_method) {
            set_headers.push(overwrite(":method", method));
        }
        if let Some(path) = self.path.as_deref().filter(|p| *p != orig_path) {
            set_headers.push(overwrite(":path", path));
        }
        if let Some(cluster) = self.cluster.as_deref() {
            set_headers.push(overwrite(CLUSTER_HEADER, cluster));
        }
        // Upstream host from the placement endpoint → `:authority`, for Envoy's
        // dynamic-forward-proxy. Unused when the route targets a normal cluster.
        if let Some(host) = self.host.as_deref() {
            set_headers.push(overwrite(":authority", host));
        }
        let body_mutation = self.body.map(|body| BodyMutation {
            mutation: Some(body_mutation::Mutation::Body(body)),
        });
        // A changed body invalidates the client's `content-length`; drop it so
        // Envoy recomputes from the mutated buffer (else it rejects the mismatch).
        if body_mutation.is_some() {
            remove_headers.push("content-length".to_owned());
        }
        let header_mutation = if set_headers.is_empty() && remove_headers.is_empty() {
            None
        } else {
            Some(HeaderMutation {
                set_headers,
                remove_headers,
            })
        };
        Ok(CommonResponse {
            status: 0, // CONTINUE
            header_mutation,
            body_mutation,
            // Do NOT clear the route cache: with the static single-cluster route,
            // re-routing is unnecessary, and clearing it mid-request causes Envoy
            // to re-match on the transiently-empty `:path` (a `no route match for
            // URL ''` → 404). The `:path` mutation still applies to the forwarded
            // request. Header-driven multi-cluster re-routing is M2c (blocked on
            // Envoy's ext_proc routing timing).
            clear_route_cache: false,
            ..Default::default()
        })
    }
}

impl EnvoyActions for ExtProcActions {
    fn set_upstream_cluster(&mut self, cluster: &str) {
        self.cluster = Some(cluster.to_owned());
    }
    fn set_upstream_host(&mut self, host: &str) {
        self.host = Some(host.to_owned());
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
