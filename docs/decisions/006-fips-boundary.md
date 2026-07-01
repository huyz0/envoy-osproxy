# ADR-006: FIPS is Envoy's wire responsibility; the extension links no data-path crypto

**Status:** Accepted

## Context

osproxy owns its wire, so FIPS compliance is osproxy's problem: its ADR-004 pins
`rustls` to a FIPS-approved cipher suite set behind a build-time `CryptoProvider`
seam (`ring` for the non-FIPS build, `aws-lc-rs`/AWS-LC-FIPS for the FIPS build),
and `cargo xtask check-fips` proves the FIPS module is engaged. The validated
module (AWS-LC-FIPS) is *inside osproxy's binary*.

In the Envoy port, Envoy owns the wire (ADR-001, ADR-002): it terminates
downstream TLS/mTLS, re-originates upstream TLS, and is the only thing that
touches the client's or the cluster's bytes on the network. Our extension either
runs out-of-process (ext_proc, reached over a localhost/UDS gRPC hop) or
in-process as a dynamic module — in neither case does it perform data-plane TLS.

So the FIPS obligation for the **wire** moves out of our code entirely. The
question is where — if anywhere — crypto legitimately remains ours.

## Decision

**FIPS for the data-plane wire is satisfied by running a FIPS-validated Envoy
(Envoy built against the BoringSSL FIPS module). Our shipped extension links no
wire crypto at all, and we enforce that in the gate.**

Concretely:

1. **Data-plane TLS/mTLS → Envoy.** Downstream and upstream TLS, cipher-suite
   policy, and client-cert validation are Envoy's `DownstreamTlsContext` /
   `UpstreamTlsContext`, backed by Envoy's BoringSSL-FIPS build. Nothing in
   `evoxy-*` negotiates TLS. The client identity reaches us already validated, as
   the XFCC header (ADR / M4), not as a certificate we parse.

2. **The Envoy ↔ ext_proc hop.** In the sidecar deployment this is a Unix domain
   socket (or loopback) with no TLS, so there is no crypto on our hop. If a
   deployment puts the ext_proc service on a remote host, that leg's TLS is again
   Envoy's `envoy_grpc` transport socket on one side and a FIPS provider on ours —
   an *opt-in* transport concern, never linked by default.

3. **The extension links no wire crypto — enforced.** `cargo xtask crypto-free`
   asserts every shipped `evoxy-*` crate's non-dev dependency tree contains no
   `rustls`/`ring`/`aws-lc-*`/`openssl`/`boring`/`native-tls`. Today it passes
   because `tonic` is used without its `tls` feature. This prevents a stray
   dependency (a `tonic` `tls` feature flip, a rustls pull-in) from silently
   putting an unvalidated crypto module in our binary and creating a FIPS
   obligation we did not intend. Test-only crypto (the mTLS e2e's `rcgen`/`reqwest`)
   is a dev-dependency and excluded.

4. **App-level crypto (future) keeps osproxy's seam.** If evoxy later adds the
   HMAC-verified diagnostics-directive / cursor tokens (M7, osproxy's
   `cert_fingerprint`/HMAC path), that is *application* crypto, not the wire: it
   reuses osproxy's build-time `CryptoProvider` selection (`ring` vs `aws-lc-rs`)
   exactly as osproxy's ADR-004 does, and would extend `crypto-free` to allow
   `aws-lc-rs` **only** on that opt-in path. Until then, we ship zero crypto.

## Consequences

- The heavy part of osproxy's M6 (pinning rustls suites, a runtime FIPS-engaged
  assertion, an AWS-LC-FIPS module in our binary) **does not port** — Envoy carries
  it. Our M6 is a boundary decision plus a gate, not a crypto build.
- Operators get a validated wire by choosing a FIPS Envoy image; we neither help
  nor hinder that, and cannot accidentally undermine it, because we link no crypto.
- The `crypto-free` gate is a standing invariant: adding wire TLS to the extension
  is now a deliberate, reviewed act (it fails the gate) with its own ADR, not an
  accident of a feature flag.
- The CMVP/consuming-a-cert nuance from osproxy (using AWS's FIPS cert is not a
  NIST engagement) applies to whoever builds the FIPS Envoy, not to us.
