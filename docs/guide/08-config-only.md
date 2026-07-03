# Configuration-only mode (no code)

For a large share of multi-tenant OpenSearch setups you write no Rust. The built-in
reference tenancy is a full multi-tenancy driven entirely by the Envoy
`filter_config` blob: pick an isolation model, set a few keys, and deploy. You only
implement a [`TenancySpi`](02-tenancy.md) when your needs go past what the keys can
express, and the last section says exactly when that is.

Every mode below is exercised live against a real Envoy and OpenSearch, so these are
tested paths, not aspirational config.

## Can I skip the SPI?

Yes, if all of these hold:

- The tenant is identified by a request header or the mTLS principal.
- Isolation is one of the three models below (dedicated cluster, dedicated index, or
  shared index).
- The physical index depends only on the tenant, not on the tenant *and* the client's
  index name.
- Placement is fixed config, not looked up from a store at request time.

If you need derived or composite tenant keys, a physical index that mixes the tenant
with the client's index name, or placement from a store, jump to
[Implementing a tenancy](02-tenancy.md).

## Isolation models

Choose one with the `isolation` key. When it is absent it is inferred: `shared_index`
if you set a shared index name, otherwise `dedicated_cluster`.

### dedicated_cluster (default)

The client's index name is kept. Requests route to one cluster, or to a per-tenant
cluster or endpoint. Use this for a single shared cluster, or for a cluster per
tenant.

```json
{ "partition_header": "x-tenant", "cluster": "opensearch" }
```

Per-tenant clusters (the filter sets `x-evoxy-cluster`, Envoy's header-matched routes
pick the upstream, see [Building the dynamic module](04-build-module.md)):

```json
{
  "partition_header": "x-tenant",
  "cluster_by_partition": { "acme": "opensearch_eu", "globex": "opensearch_us" }
}
```

### dedicated_index

One cluster, a per-tenant physical index from `index_template` (`{partition}` is
substituted). Isolation is physical, by index. No injected field, no id rewrite.

```json
{
  "isolation": "dedicated_index",
  "partition_header": "x-tenant",
  "index_template": "orders-{partition}"
}
```

A write to the logical `/orders/_doc/1` lands in `orders-acme` for tenant `acme`, and
a read comes back in the logical view. The template can prefix (`orders-{partition}`)
or be the bare partition (`{partition}`, the default). It cannot include the client's
logical index; that combination needs the SPI.

### shared_index

All tenants share one physical index, isolated by an injected field set to the
partition and a partition-scoped, reversible doc id. The read path strips the field
and maps the id back, so each tenant sees only its own data in its own logical view.

```json
{
  "isolation": "shared_index",
  "partition_header": "x-tenant",
  "shared_index": "orders_shared",
  "inject_field": "_tenant",
  "id_template": "{partition}:{body.id}",
  "routing": true
}
```

## Tenant source

Set with `partition_source` (or the shortcut keys):

- `header` (default): read the tenant from `partition_header`.
- `principal`: read it from the Envoy-validated mTLS identity, which a client cannot
  spoof with a request header. Set `"partition_source": "principal"`.
- `path`: the tenant is the first path segment. The filter strips it and moves it
  into `partition_header` before routing, so `/acme/orders/_doc/1` resolves tenant
  `acme` with path `/orders/_doc/1`. Set `"partition_source": "path"`.
- `default_partition`: a fallback used when the source is missing, instead of failing
  closed. This is how you run single-tenant (default everything to one partition) or
  degrade gracefully. Without it, an unresolved tenant is a fail-closed `400`.

## Passthrough indices

List logical indices in `passthrough_indices` to exempt them from tenancy. A request
for one is forwarded unchanged, with no partition required, no transform, and no
cluster override, to Envoy's default route. This is for a global or shared index that
every tenant reads without isolation, like a product catalog.

```json
{ "partition_header": "x-tenant", "passthrough_indices": ["catalog", "reference"] }
```

A write or read of `/catalog/...` passes straight through; `/orders/...` still routes
by tenant. Note that passthrough is checked after the `path` source rewrite, so if
you use both, the passthrough name is the index that remains once the tenant segment
is stripped.

## Upstream selection

These apply across all isolation models:

- `cluster`: the default upstream cluster.
- `cluster_by_partition`: per-tenant cluster names, for header-matched routing.
- `endpoint` / `endpoint_by_partition`: per-tenant upstream URLs, dialed by Envoy's
  dynamic-forward-proxy with no cluster defined. This is the path for per-tenant AWS
  ALBs; see [Building the dynamic module](04-build-module.md).

## Full key reference

| Key | Type | Default | Meaning |
|---|---|---|---|
| `isolation` | string | inferred | `dedicated_cluster`, `dedicated_index`, or `shared_index` |
| `partition_source` | string | `header` | `header`, `principal`, or `path` |
| `partition_header` | string | `x-tenant` | header carrying the tenant (header source, or where `path` puts it) |
| `default_partition` | string | none | fallback tenant when the source is missing |
| `passthrough_indices` | array | `[]` | logical indices forwarded unchanged, bypassing tenancy |
| `cluster` | string | `opensearch` | default upstream cluster |
| `cluster_by_partition` | object | `{}` | per-tenant cluster names |
| `endpoint` | string | `http://localhost:9200` | default upstream URL (dynamic-forward-proxy) |
| `endpoint_by_partition` | object | `{}` | per-tenant upstream URLs |
| `index_template` | string | `{partition}` | per-tenant physical index (`dedicated_index`) |
| `shared_index` | string | none | shared physical index (`shared_index`) |
| `inject_field` | string | `_tenant` | injected isolation field (`shared_index`) |
| `id_template` | string | `{partition}:{body.id}` | partition-scoped doc id (`shared_index`) |
| `routing` | bool | `true` | set `?routing=` in `shared_index` mode |
| `admin_token` | string | none | bearer token that enables the `/_evoxy/admin/directives` plane (see [Admin and observability](07-observability.md)) |
| `emit_decision` | bool | `true` | initial state of the `x-evoxy-decision` response header |

Unknown keys are ignored, and any missing key falls back to its default, so a bare
`{}` is a runnable single-cluster passthrough. `admin_token`/`emit_decision` are the
reserved observability keys; they enable the introspection/admin surfaces (on the
module) from the same blob, so admin is config-only too.

## What still needs a custom SPI

Config-only cannot express:

- A physical index that mixes the tenant with the client's index name (for example
  `{tenant}_{index}`). The placement API is given only the tenant, not the index.
- Placement looked up at runtime from etcd, a database, or any store.
- A tenant derived from a JWT claim, the request body, or a composite of several
  inputs.
- A live migration state machine driven by a control plane.

Each of those is a short `TenancySpi` implementation. Start at
[Implementing a tenancy](02-tenancy.md); the reference tenancy's source is a working
example to copy from.
