# envoy-osproxy. Technical Analysis: extending Envoy without rebuilding it


## 1. Goal

Deliver the osproxy capability set, multi-tenant isolation, request/response
body reshaping, `_bulk` NDJSON demux, epoch-gated migration, read-only
shape-only observability with runtime directives, traffic capture, but built as
an **extension of Envoy** rather than a from-scratch proxy, and crucially
**without forking or recompiling the Envoy C++ binary**. Operators should run a
*stock* Envoy release and drop our logic in as configuration + a loadable
artifact.

## 2. The key enabler: osproxy is already transport-agnostic

osproxy did not build one monolith. It split the wire from the brain:

| Layer | Crate | Responsibility |
|-------|-------|----------------|
| **Transport** | `osproxy-transport` | HTTP/1.1 + H2 + gRPC framing, TLS/mTLS termination (behind `CryptoProvider`), pooled upstream connections, admission limits, circuit breaking |
| **Brain** | `osproxy-engine` (+ `-rewrite`, `-tenancy`, `-observe`, `-sink`, `-spi`, `-core`) | Everything semantic: authn/authz classification, tenant resolution, body rewrite, bulk demux, migration epoch gate, observability |

The brain's entire input is one plain data struct:

```rust
// osproxy-spi::RequestCtx
principal, request_id, method, endpoint, protocol,
logical_index, doc_id, headers, body, query, path, forward_headers
```

and its output is `PipelineResponse { status, body, content_type }`. The engine
entry point is:

```rust
Pipeline::handle(&self, ctx: &RequestCtx<'_>) -> Result<PipelineResponse, RequestError>
```

**This is the whole thesis.** Every field of `RequestCtx` is reconstructable
from what *any* Envoy extension point already hands a filter (method, path,
authority, headers, buffered body). So porting to Envoy is not a rewrite of the
hard part, it is **replacing `osproxy-transport` with Envoy** and re-hosting the
existing, tested engine crates behind whichever Envoy extension seam we pick.
Envoy becomes the transport crate; our Rust crates stay the brain.

Corollary: Envoy *already provides, at production quality*, most of what
`osproxy-transport` and `osproxy-server` hand-built. TLS, HTTP/2, connection
pooling, health checking, circuit breaking, load balancing, retries, rate
limiting, access logging, stats. Adopting Envoy is largely **deleting** our
transport/ops surface and inheriting Envoy's, keeping only the part that is
genuinely ours: the tenancy/rewrite/observe brain.

## 3. The four "no-rebuild" extension mechanisms

Envoy exposes four ways to inject custom L7 logic into a stock binary. Ranked by
fit for this workload:

### 3.1 External Processing, `ext_proc` (out-of-process gRPC), **recommended primary**

Envoy's HTTP filter streams request/response headers and body over a
bidirectional gRPC stream to an **external service you write** (in Rust). That
service returns mutations: rewritten headers, replaced/streamed body, or an
immediate canned response. Envoy owns the socket; your service owns the
semantics.

- **Fit:** near-perfect. Our `ext_proc` server is a thin adapter that maps an
  Envoy `ProcessingRequest` → `RequestCtx`, calls `Pipeline::handle`, and maps
  `PipelineResponse` → `ProcessingResponse`. The osproxy engine crates are
  reused *verbatim*, we already speak "headers + body in, response out."
- **No rebuild:** `ext_proc` ships in the stock Envoy image; it is pure
  bootstrap config (`http_filters: - name: envoy.filters.http.ext_proc`).
- **Process isolation:** our code crashing cannot take Envoy down; deploy/scale
  independently; language-free.
- **Cost:** one gRPC hop per request (localhost/UDS) + serialization of headers
  and, for body mutations, the body. This is the main perf question (see §6).
- **Body handling:** supports buffered *and* streamed modes; streamed is what we
  need for `_bulk` to preserve osproxy's bounded-memory NDJSON demux (ADR-014).

### 3.2 Dynamic Modules (in-process Rust `.so`), **recommended for a v2 / low-latency tier**

Envoy ≥ 1.34 loads a **shared library at runtime** through a stable C ABI, with
an official Rust SDK. No gRPC hop, the filter runs in-process in the worker
thread. Configured via the `dynamic_modules` field on an HTTP filter; the stock
Envoy binary `dlopen`s our `libenvoy_osproxy.so`.

- **Fit:** excellent for latency; the engine crates link straight into the
  module. Same `RequestCtx` adapter, no serialization, no extra hop.
- **No rebuild:** the ABI is exactly the "extend without recompiling Envoy"
  contract, stock binary + our `.so`.
- **Cost/risk:** in-process means a panic/UB in our code is an Envoy worker
  crash, the engine's `#![deny(unwrap_used/expect_used/panic)]` lint posture
  (already enforced in osproxy lib crates) becomes a hard safety requirement, not
  just hygiene. ABI is versioned against the Envoy minor; upgrades must match.
- **Maturity:** newer than `ext_proc`; the C ABI evolves across Envoy minors,
  so an in-process build pins an Envoy version floor. **Resolved for this project:
  we track *latest* Envoy, so ABI churn is a non-issue and the dynamic module is a
  first-class primary from M0.**
- **Language:** the ABI is a C ABI over a `.so`. The only first-class, maintained
  SDK is **Rust** (`envoy-proxy-dynamic-modules-rust-sdk`), which is also exactly
  what lets us link the osproxy engine crates in with zero language boundary. (C/C++
  can implement the raw ABI by hand; Go via `-buildmode=c-shared`+cgo is possible
  but unergonomic. Note the separate `contrib` Go *HTTP filter* is a different
  mechanism and is **not** stock-binary-loadable.) We write the module in Rust.

### 3.3 proxy-wasm (in-process WASM sandbox), **rejected as primary**

Compile a filter to WASM (`proxy-wasm-rust-sdk`), load into Envoy's sandbox.
Portable and memory-safe, but: (a) the sandbox blocks arbitrary syscalls and
constrains crypto, our HMAC directive verification / FIPS story does not port
cleanly; (b) per-call VM boundary + body copies add overhead exactly on the
allocation/body-heavy path osproxy is tuned for; (c) large streamed bodies
(`_bulk`) are awkward in the host↔guest ABI. Good for tiny header logic, wrong
tool for a body-reshaping tenancy engine.

### 3.4 ext_authz / Lua, **complementary, not sufficient**

`ext_authz` (external authorization) only yields an allow/deny + header mutation
,  it can host the *isolation admission* decision but cannot rewrite bodies or
demux `_bulk`. Lua is inline but too limited for our engine. Useful as an
*adjunct* (e.g. a fast reject tier) but cannot carry the whole port.

### Recommendation

Because we track **latest Envoy**, the dynamic-module ABI-pin caveat is moot, so
either seam is viable from M0. Both sit behind the **same `RequestCtx` adapter**,
so the choice is a deployment knob, not a rewrite (as osproxy already treats
transport as swappable):

- **Rust dynamic module (§3.2), primary when latency is the priority.** In-process,
  no gRPC hop, osproxy's own latency profile, mimalloc applies directly, full engine
  reuse. Cost: a filter panic/UB crashes the Envoy worker → the engine's
  `deny(unwrap_used/expect_used/panic)` posture is now a hard safety requirement;
  couples our deploy/scale to Envoy's; harder to debug in isolation.
- **`ext_proc` (§3.1), primary when isolation/operability is the priority.** Process
  isolation (our crash can't take Envoy down), independent scale/deploy, easiest to
  test, at the cost of one localhost/UDS hop.

Build the `RequestCtx` adapter first; pick the backend per environment.

## 4. Capability mapping: who owns what after the port

| osproxy capability | In envoy-osproxy | Owner |
|---|---|---|
| TLS / mTLS termination | Envoy `transport_socket` (BoringSSL) | **Envoy** |
| HTTP/1.1, H2, gRPC ingress | Envoy listeners/codecs | **Envoy** |
| Upstream connection pooling, circuit breaking, retries, LB, health checks | Envoy clusters | **Envoy** |
| Admission limits (413/429), timeouts | Envoy (`buffer`, limits) + our engine | **Both** |
| Tenant isolation / routing (fail-closed) | `osproxy-tenancy` behind our filter | **Us** |
| Body rewrite, id injection, query rewrite | `osproxy-rewrite` | **Us** |
| `_bulk` NDJSON demux + per-item response | `osproxy-engine::bulk` (streamed body) | **Us** |
| `_mget` / `_msearch` demux | `osproxy-engine` | **Us** |
| Epoch-gated migration + write gate | `osproxy-tenancy::migration` | **Us** |
| Async fan-out write mode | `osproxy-engine::asyncwrite` + capture | **Us** |
| Shape-only observability, `/debug/explain`, break-glass, directives, `/admin/directives`, `/metrics` | `osproxy-observe` exposed via our service's admin port (not Envoy) | **Us** |
| W3C trace propagation, OTLP export | Envoy native tracing **and/or** our `osproxy-otlp` | **Both**, reconcile (§5) |
| Access logs / stats | Envoy native | **Envoy** (our shape-only logs become adjunct) |
| Traffic capture (Kafka/WAL tee) | `osproxy-capture` in our service | **Us** |

Net: the port **deletes** a large slice of osproxy (transport, TLS provider
plumbing, ingress servers, pool telemetry, much of `osproxy-server`) and
**keeps** the crates that encode the actual product, tenancy, rewrite, engine,
observe, sink, capture, spi, core.

## 5. Boundaries that shift and need decisions

1. **Crypto / FIPS boundary moves to Envoy.** Today FIPS is our aws-lc-rs build
   (ADR-004). With Envoy terminating TLS, the *transport* FIPS boundary becomes
   **Envoy's BoringSSL FIPS build**, a separate, well-trodden validation. Our
   remaining crypto (HMAC directive tokens, cursor signing) stays in our Rust and
   keeps the aws-lc-rs/ring seam. Decision: adopt Envoy-FIPS for the wire; keep
   our seam only for app-level HMAC. This likely *simplifies* our FIPS story.
2. **Two tracing stacks.** Envoy emits spans; so does `osproxy-otlp`. Pick one
   authoritative exporter (recommend Envoy for the transport span, our engine
   adds shape-only attributes / child span) to avoid double-reporting.
3. **The admin/introspection plane is ours, not Envoy's.** `/debug/explain`,
   `/debug/breakglass`, `/admin/directives`, `/metrics` are osproxy semantics
   Envoy has no notion of, they stay served by *our* service on its own port,
   unchanged. (With `ext_proc` this is trivially just another route on our gRPC
   service's sidecar HTTP; with a dynamic module we need a small companion.)
4. **mTLS identity for `principal`.** Today `RequestCtx.principal` comes from our
   authenticator over the client cert / bearer. In Envoy, the client cert is
   terminated by Envoy, we receive identity via forwarded metadata
   (`x-forwarded-client-cert`/SAN, or filter metadata), so the authenticator
   adapts from "parse the cert myself" to "trust Envoy's validated identity."

## 6. Performance implications (carry the osproxy findings forward)

osproxy's tuning arc concluded the proxy is **allocation-bound, not lock-bound**,
and shipped mimalloc for a ~25% peak-throughput win at high fan-in (ADR-015).
Those findings transfer with adjustments:

- **`ext_proc` adds a localhost gRPC hop.** osproxy measured added p50 ≈ 0.08 ms
  / p99 ≈ 1.7 ms of *its own* overhead; the ext_proc hop adds serialization +
  IPC on top. Mitigations: Unix domain socket transport, header-only processing
  where the body is untouched (skip body streaming when the route needs no
  rewrite, most reads), and Envoy's `request_body_mode: STREAMED` only for
  `_bulk`/writes. Budget this against NFR-P1 explicitly on real hardware.
- **The dynamic-module tier removes the hop** and brings us back to osproxy's
  in-process profile; mimalloc still applies (set the module/service global
  allocator). The allocation-bound conclusion is unchanged, the body-reshaping
  work is the same regardless of who owns the socket.
- **Envoy owns pooling now**, so osproxy's pool-reuse scalability result
  (~44× throughput, flat p50) is subsumed by Envoy's mature connection
  management, one less thing for us to defend, and likely better than ours.
- **Reuse the `osproxy-bench` harness.** `NfrProfile`/`judge()`/scalability/
  footprint gating are transport-agnostic; point the load runner at
  Envoy+our-service instead of the standalone binary and the same JSON verdicts
  gate the port. This is our A/B proof that the Envoy path meets NFR-P.

## 7. Proposed architecture (ext_proc primary)

```
        client
          │  TLS/mTLS, HTTP/1.1|H2|gRPC
          ▼
   ┌──────────────┐   ext_proc gRPC (UDS)   ┌───────────────────────────┐
   │  stock Envoy │◄──────────────────────► │ envoy-osproxy service (Rust)│
   │  (unmodified)│   headers+body ⇄ mutations│  adapter → RequestCtx →    │
   └──────┬───────┘                          │  osproxy-engine::Pipeline  │
          │ upstream cluster                 │  (+tenancy/rewrite/observe)│
          ▼                                  │  admin port: /debug /admin │
     OpenSearch                              └───────────────────────────┘
```

The Rust service is a **new thin crate** (`envoy-osproxy` / an `ext_proc`
adapter) plus a **git dependency / vendored subset of the osproxy engine
crates**. No changes to Envoy source. Deployment is a stock Envoy image + our
service container + one bootstrap YAML.

## 8. Milestone plan (mirrors osproxy's M1→M7 discipline)

- **M0, spike (this analysis + a walking skeleton):** stock Envoy + a Rust
  `ext_proc` server that echoes; prove header→`RequestCtx` mapping and the
  bootstrap wiring end-to-end against a real OpenSearch container.
- **M1, single-doc write path** through `Pipeline::handle` (reuse engine).
- **M2, read path** (get/delete/search/count).
- **M3, `_bulk`/`_mget`/`_msearch`** with STREAMED body mode (bounded memory).
- **M4. TLS/mTLS via Envoy** + principal-from-Envoy-metadata; delete our
  transport/TLS surface.
- **M5, migration epoch gate + async fan-out.**
- **M6. FIPS:** adopt Envoy-BoringSSL-FIPS for the wire; keep app-HMAC seam.
- **M7, observability:** admin plane on our port; reconcile tracing with Envoy;
  reuse `osproxy-bench` for the NFR-P A/B verdict.
- **(v2), dynamic-module backend** behind the same adapter for the low-latency
  tier.

## 9. Open questions for the user

1. **Deployment shape:** is the target a sidecar-per-instance, a shared gateway,
   or an Istio/service-mesh filter? (Affects ext_proc vs dynamic-module default.)
2. **Reuse strategy for the engine crates:** git-dependency on `opensearch-proxy`,
   a shared workspace, or vendor-and-slim? (Recommend git-dep to start; the
   engine is already a clean, versioned public surface.)
3. **Latency budget:** is the ext_proc hop acceptable for v1, or is the
   dynamic-module in-process path required from the start?
4. **Envoy version floor:** pin to a minor with stable `ext_proc` (and, if we go
   in-process, ≥ the dynamic-modules ABI you want to target).

---

**Bottom line:** because osproxy already isolated its brain behind
`RequestCtx`/`Pipeline`, "extend Envoy without rebuilding it" reduces to *swap
the transport crate for a stock Envoy and re-host the same engine behind
`ext_proc`* (ship now, full reuse, isolated) *with a dynamic-module fast path as
a later drop-in*. We inherit Envoy's production transport and delete most of
ours; the tenancy/rewrite/observe value stays ours and moves over intact.
