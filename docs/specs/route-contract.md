# Spec — transform-then-forward route contract

Normative spec for `evoxy-route` (`ROUTE-*`), the code-side of ADR-002. Each rule
is traced by a test in `crates/evoxy-route/`. Change the rule and its test
together.

## Reuse boundary (ROUTE-R*)

- **ROUTE-R1** — Resolution reuses the osproxy engine: `Router::resolve(ctx)`
  derives partition + placement + `RouteDecision` (never re-implemented here).
- **ROUTE-R2** — The body transform reuses the `osproxy-rewrite` byte-splice
  primitives (`inject_fields_bytes`, `construct_id_bytes`, `validate_json`); only
  the forward-shaped glue is local.
- **ROUTE-R3** — The route path **never dispatches** (no `Sink`, no upstream
  client); it yields a `PreparedForward` for Envoy or a fail-closed response.

## Forward construction (ROUTE-F*)

- **ROUTE-F1** — `PreparedForward.cluster` is `RouteDecision.target.cluster` (the
  logical `ClusterId`), for the filter to map to an Envoy upstream cluster.
- **ROUTE-F2** — The physical index is `RouteDecision.target.index`: a dedicated
  cluster keeps the logical index name; dedicated/shared index modes pin the
  physical index (per the engine's `target_for`).
- **ROUTE-F3** — The physical doc id is the transform-constructed id when present,
  else the client's path id (`ctx.doc_id()`). Method is `PUT` when an id is known,
  else `POST`.
- **ROUTE-F4** — `?routing=<partition>` is appended iff the id rule set routing.
- **ROUTE-F5** — Injected fields are spliced into the forwarded body; a passthrough
  (no transform) forwards the body verbatim after validating it is a JSON object.

## Fail-closed mapping (ROUTE-E*)

Every error becomes a `Forward::Immediate` with a shape-only body
`{"error":"<code>"}` (a stable code, never a tenant value).

| id | Condition | Status | code |
|----|-----------|--------|------|
| ROUTE-E1 | endpoint not yet supported (non-`IngestDoc` at M1) | 501 | `endpoint_not_supported_yet` |
| ROUTE-E2 | `SpiError::PartitionUnresolved` / principal-attr / header missing | 400 | `partition_unresolved` / … |
| ROUTE-E3 | `SpiError::PlacementMissing` / `PlacementBackend` | 503 | `placement_missing` / `placement_backend` |
| ROUTE-E4 | `SpiError::UnsupportedEndpoint` | 501 | `unsupported_endpoint` |
| ROUTE-E5 | `SpiError::IdRuleMissingPartition` | 500 | `id_rule_missing_partition` |
| ROUTE-E6 | body rewrite failure (not an object, reserved collision, id template) | 400 | `body_rewrite_failed` |
| ROUTE-E7 | injected value unresolved (invariant break) | 500 | `unresolved_injected_value` |

- **ROUTE-E0 (invariant)** — an `Unknown`/unsupported endpoint never routes blind;
  it fails closed (INV-4).

## Read path (ROUTE-R*, M2a)

Request-side only; response-side field-strip/id-unmap is M2b.

- **ROUTE-RD1** — `GetById`/`DeleteById`: the physical id is the id rule applied
  to the client's logical id (`map_logical_to_physical`) when the placement
  constructs ids, else the client id unchanged; forwarded to
  `/{physical_index}/_doc/{physical_id}` with the client method and no body,
  `?routing=` appended when the rule sets it.
- **ROUTE-RD2** — `Search`/`Count`: forwarded as `POST /{physical_index}/_search`
  or `/_count`. The mandatory partition filter (`wrap_query` over the injected
  `PartitionId` field) is applied for a shared index — the read isolation boundary
  (ADR-006); a dedicated placement (no isolation field) passes the query through.
- **ROUTE-RD3** — an empty search body becomes `{}` before the filter is applied,
  so a bodyless search on a shared index is still isolated.

> Coverage: `route_tests.rs` covers ROUTE-F1..F5, ROUTE-E1/E2/E6, and ROUTE-RD1/RD2
> (dedicated get-by-id + `SharedIndex` id-map + dedicated/shared search + count);
> `tests/e2e.rs` reads the written doc back through Envoy (write→read round-trip).
