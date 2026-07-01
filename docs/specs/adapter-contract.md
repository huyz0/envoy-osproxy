# Spec. Envoy→engine adapter contract

Normative spec for `evoxy-adapter`. Each rule has a stable id (`ADAPT-*`) that a
test references, so the mapping is spec-driven, not incidental. Change the rule
and the test together.

## Principal (ADAPT-P*)

- **ADAPT-P1**, When Envoy presents a validated client certificate
  (`MtlsIdentity.presented == true`) with a non-empty stable id, the principal id
  is that stable id: the **first URI SAN** if any, else the **subject DN**.
- **ADAPT-P2**. Otherwise the principal id is the constant `anonymous`. The
  adapter never rejects on identity; authorization is the engine's job (INV-4).

## Protocol (ADAPT-PR*)

- **ADAPT-PR1**, `content-type` starting `application/grpc` ⇒ `Protocol::Grpc`,
  regardless of HTTP version.
- **ADAPT-PR2**. Else `HttpVersion::Http11 → Http1`, `Http2 → Http2`.

## Method (ADAPT-M*)

- **ADAPT-M1**, `GET|PUT|POST|DELETE|HEAD` map to the matching `HttpMethod`.
- **ADAPT-M2**. Any other method is `AdaptError::UnsupportedMethod` (fail-closed).

## Endpoint classification (ADAPT-E*)

Path is the `:path` with `?query` stripped, split on `/` (empty segments dropped).

| id | Path shape | Method | `EndpointKind` | index | doc_id |
|----|-----------|--------|----------------|-------|--------|
| ADAPT-E1 | `/{i}/_doc/{id}` | PUT/POST | `IngestDoc` | `{i}` | `{id}` |
| ADAPT-E2 | `/{i}/_doc/{id}` | GET/HEAD | `GetById` | `{i}` | `{id}` |
| ADAPT-E3 | `/{i}/_doc/{id}` | DELETE | `DeleteById` | `{i}` | `{id}` |
| ADAPT-E4 | `/{i}/_create/{id}`, `/{i}/_update/{id}` | any | `IngestDoc` | `{i}` | `{id}` |
| ADAPT-E5 | `/{i}/_bulk`, `/_bulk` | any | `IngestBulk` | `{i}`/`""` | n/a |
| ADAPT-E6 | `/{i}/_search`, `/_search` | any | `Search` | `{i}`/`""` | n/a |
| ADAPT-E7 | `/{i}/_count`, `/_count` | any | `Count` | `{i}`/`""` | n/a |
| ADAPT-E8 | `/{i}/_mget`, `/_mget` | any | `MultiGet` | `{i}`/`""` | n/a |
| ADAPT-E9 | `/{i}/_msearch`, `/_msearch` | any | `MultiSearch` | `{i}`/`""` | n/a |
| ADAPT-E10 | `/{i}/_delete_by_query` | any | `DeleteByQuery` | `{i}` | n/a |
| ADAPT-E11 | `/{i}/_pit`, `/_search/scroll` | any | `Cursor` | `{i}`/`""` | n/a |
| ADAPT-E12 | `/_cat/*`, `/_cluster/*`, `/_nodes/*`, `/_aliases`, `/_tasks/*`, `/_snapshot/*` | any | `Admin` | `""` | n/a |
| ADAPT-E13 | anything else, incl. bare `/{i}` and `/` | any | `Unknown` | `{i}`/`""` | n/a |

- **ADAPT-E0 (invariant)**, classification never fails; unmatched shapes are
  `Unknown`, which the pipeline rejects unless pass-through is configured. This is
  the fail-closed default (INV-4).

## Fidelity (ADAPT-F*)

- **ADAPT-F1**, `RequestParts::ctx()` reproduces the request faithfully: method,
  endpoint, protocol, logical index, doc id, query, path, headers, and body all
  match the input `FilterRequest`.
- **ADAPT-F2**, The `RequestId` is Envoy's `x-request-id`, passed through
  verbatim so the request is traceable across the hop (docs/09).

> Coverage: `crates/evoxy-adapter/src/classify.rs` tests cover ADAPT-E1…E13;
> `crates/evoxy-adapter/src/lib.rs` tests cover ADAPT-P*, ADAPT-PR*, ADAPT-M*,
> ADAPT-F*. Extend both when a rule changes.
