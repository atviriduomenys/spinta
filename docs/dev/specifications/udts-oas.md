# UDTS data service OpenAPI specification

How Spinta generates the OpenAPI specification of one UDTS data service from a
manifest. User guide (command, configuration, work in the gateway):
[`docs/lt/agentas/oas-generavimas.md`](../../lt/agentas/oas-generavimas.md).

## Purpose

The API gateway uses the specification for two things:

1. **importing endpoints**: the gateway builds an API from it and takes the API
   context path from the path of the first `servers` entry;
2. **validating requests and responses**: the *OpenAPI Specification
   Validation* policy in the Request and Response phases.

So the specification has to match what Spinta actually accepts and answers: as
strict as possible on the request side, never stricter than Spinta can answer
on the response side.

## Code and tests

| Where | What |
|---|---|
| `spinta/cli/udts/oas.py` | CLI command `spinta udts oas` |
| `spinta/manifests/open_api/service.py` | data service path logic |
| `spinta/manifests/open_api/udts_config.py` | reading and checking `--udts-cfg` (`UdtsConfig`) |
| `spinta/manifests/open_api/udts_cfg.example.yml` | example configuration |
| `spinta/manifests/open_api/openapi_generator.py` | the generator (`OpenAPIGenerator`) |
| `spinta/manifests/open_api/openapi_config.py` | templates of paths, parameters, responses, schemas |
| `spinta/manifests/open_api/helpers.py` | `create_openapi_manifest`, `write_openapi_manifest` |
| `spinta/spyna.py` | `_page=<token>` query syntax |
| `tests/manifests/open_api/` | generator, configuration and validity tests |
| `tests/cli/test_udts_oas.py` | CLI tests |

Related ADRs: [ADR-0001](../../adr/0001-oas-429-open-object.md) (`429`),
[ADR-0002](../../adr/0002-oas-integer-bounds-not-stated.md) (integer bounds),
[ADR-0003](../../adr/0003-oas-version-3-0.md) (OpenAPI 3.0),
[ADR-0004](../../adr/0004-oas-empty-identifier-not-described.md) (empty
identifier).

## Inputs

### Modes

`create_openapi_manifest` works in two modes:

| Mode | Called with | `servers` | Agent endpoints | Schema names |
|---|---|---|---|---|
| **Data service** | `service_path=…` (`spinta udts oas`) | from `--udts-cfg` | both forms | path within the service |
| **Catalog** | no `service_path` (whole manifest or `main_dataset_name`) | none | agent addresses only | full name or `basename` |

The rest describes the data service mode; catalog differences are in the table.

### Data service path

A data service is the leading part of a dataset path:

```
datasets/{form}/{org}/{is}/{service}/{version}/{dataset}/{model}
└──────────────── data service ───────────────┘
```

- `{version}` is optional; when present it is a positive integer.
- `--path` selects datasets whose data service path **equals** it, so
  `.../at280/1` does not include `.../at280/10`, and an unversioned
  `.../at280` does not include `.../at280/1`. A path of another shape is
  accepted with a warning and selects by prefix on a segment boundary.
- Without `--path`: a manifest with one data service uses it, with several it
  fails listing them. `--list` lists data services and their datasets.

### Configuration (`--udts-cfg`)

A YAML file, required to export a data service.

| Key | Required | Check |
|---|---|---|
| `info.title` | yes | non-empty string |
| `info.contact.name` | yes | non-empty string |
| `info.contact.url` | yes | absolute URL |
| `info.contact.email` | yes | see below |
| `servers` (≥ 1) | yes | `url` is `http`/`https` or relative; no templates (`{…}`) |
| `auth.token_url` | when the first server is relative | absolute `http`/`https` URL |
| `info.summary`, `info.description`, `info.version`, `info.termsOfService`, `info.license` (`name`, `url`) | no | OpenAPI types |
| `externalDocs` (`url`, `description`) | no | |
| `limits.max_limit` | no | integer 1…2⁶³−1, default 100000 |

- An unknown key is left out with a warning; `x-` extensions are kept where
  OpenAPI allows them (`info`, `info.contact`, `info.license`, `servers`
  entries, `externalDocs`).
- An `http` address is accepted with a warning (credentials would travel in
  the clear).
- **Email** is checked in its common form: RFC 5322 dot-atom words joined by
  single dots before `@`; a domain of two labels or more after it, no label
  starting or ending with `-`; up to 64 characters before `@` and 254 in all.
  Quoted local parts and address literals are not accepted.

### What is published: `visibility`

Only metadata whose `visibility` is `protected`, `package` or `public` goes into
the specification. `private` and an **empty** `visibility` (DSA defaults it to
`private`) are not published:

| Element | Not published means |
|---|---|
| model | no paths, schemas or tag |
| property | not in the schema, examples or query examples; a `file`/`object` property gets no path |
| enum value | not in the `enum` list |
| language property (`name@lt`) | the property stays while at least one language is published |
| reference to an unpublished model | points at the shared `UnpublishedReference` schema (an open object revealing nothing) |

A `UserWarning` says how many models and properties were left out, and a
separate one when no model is published. `visibility` hides metadata only; data
access is governed by `access`.

## Document structure

```
openapi: 3.0.3
info            ← --udts-cfg info (+ --api-version)
externalDocs    ← --udts-cfg externalDocs
servers         ← --udts-cfg servers, each with the data service path
tags            ← utility + one per model, sorted
paths           ← agent endpoints + model paths
components
  schemas       ← models, listings, references, shared schemas
  parameters    ← headers; per model: id, _select, _sort; per document: _limit; _page
  headers, responses, securitySchemes
```

### `info`

Taken from the configuration. OpenAPI 3.0 has no `info.summary`, so `summary`
becomes the first paragraph of `description`. In catalog mode `summary` and
`description` come from the dataset `title` and `description`.

### `servers`

Each configuration entry:

- `url` without a path → the data service path is appended
  (`https://get.data.gov.lt` → `https://get.data.gov.lt/datasets/gov/rc/jadis/at280/1`);
- `url` with the data service path → kept as is;
- `url` with another path → kept as is, with a warning.

Without a configuration (Python API only): one relative `/{data service path}`.

### Paths

**Agent endpoints** are given in two forms, marked by the path extension
`x-spinta-context`:

| Path | `x-spinta-context` | `servers` | Purpose |
|---|---|---|---|
| `/:version`, `/:health`, `/:token` | `gateway` | the document's (data service base) | how the gateway routes them inside a data service |
| `/version`, `/health`, `/auth/token` | `agent-direct` | its own: server address without a path | how the agent itself serves them |

Catalog mode gives the `agent-direct` form only. Data paths carry no
`x-spinta-context`, since both contexts serve them.

**Model paths** (relative to the data service base, `{dataset}/{Model}`):

| Path | Operations | When |
|---|---|---|
| `/{dataset}/{Model}` | `get`, `head` | always |
| `/{dataset}/{Model}/{id}` | `get`, `head` | always |
| `/{dataset}/{Model}/{id}/{property}` | `get`, `head` | `file`, `image` property; answers with binary content, supports `Range` (`206`, `416`) |
| `/{dataset}/{Model}/{id}/{property}:ref` | `get`, `head` | `file`, `image` property; answers with file metadata |
| `/{dataset}/{Model}/{id}/{property}` | `get`, `head` | `object` property; answers with the object plus `_type`, `_revision` |

**Parameters** are listed on every operation (none on the path), because the
gateway reads the operation alone.

### Query parameters

| Parameter | Component | Schema |
|---|---|---|
| `{id}` (UUID) | `id` | `UUID_REQUEST_PATTERN`: canonical, without hyphens, `{…}`, `urn:`/`uuid:` prefixes; v4 |
| `{id}` (model declares `_id`) | `id_{Schema}` | by type: `string` one segment up to 512; `base32` `=` + Base32; `integer` `int64` bounds; `enum` the values |
| `_select` | `select_{Schema}` | names, paths, functions, `*`; up to 1000 characters; example from model properties |
| `_sort` | `sort_{Schema}` | names with `+`/`-`; up to 1000 characters |
| `_limit` | `limit` | `integer`, `minimum: 1`, `maximum: limits.max_limit`, `int32` if it fits, else `int64` |
| `_page` | `page` | URL safe Base64 with `=` padding, up to 8192 characters |
| `traceparent` | | W3C Trace Context; not version `ff`, no all-zero identifiers (`not`) |
| `tracestate`, `Cache-Control`, `If-None-Match`, `Accept-Language`, `Range` | | printable ASCII, `maxLength: 1024` |

The `=` prefix of `{id}` is used when `is_accessible_by_equals_sign` holds: a
`base32` identifier, or a `string` one of a model with a single-part key.

A filter on properties, `count()` / `_count` and the call forms (`limit(…)`,
`select(…)`, `sort(…)`, `page(…)`) have no fixed name, so they are described in
the listing operation's `description`.

Spinta supports `_page=<token>` through the `spinta/spyna.py` grammar (terminal
`_page=`), the same way as `_limit=`. This is a stopgap until
[#2023](https://github.com/atviriduomenys/spinta/issues/2023). An invalid token
(not Base64, not a JSON list, not a string) is refused with `InvalidPageKey`.

### Schemas

**Names** (`SchemaNamer`): in data service mode, the model path within the
service with `_` for `/` (`at280_israsas_DalyvioAsmensIsrasas`); characters an
OpenAPI component name cannot hold become `_`; collisions get a numeric suffix.
From a model name `X`:

| Schema | What |
|---|---|
| `X` | model object |
| `XCollection` | listing: `_data` (required), `_page` |
| `X_Ref` (+ `_2`, …) | reference to `X`, one per reference shape (level, `refprops`) |
| `X_{prop}` | answer of an `object` property |
| `X_{prop}_ref` | `:ref` answer of a `file` property |

Tags and `operationId`s use the same names.

**Model schema:**

- `_type` is an `enum` of the full model name; `_id` a UUID v4 (canonical
  form) or the declared `_id` type; `_revision` a UUID or the type of the value
  the model builds.
- No `required`: a response holds what was asked for (`_select`).
- A property not marked `required` in the manifest is `nullable`; an `enum`
  gets `null` added; a reference becomes `anyOf: [{$ref}, NULL_OBJECT_SCHEMA]`.
- `integer` properties have no `format` or bounds (ADR-0002).
- A `uuid` property is lower case v4 (`UUID.load`); `base32` the RFC 4648
  alphabet without padding.

**Reference schema** (`X_Ref`):

- level ≥ 4 → `{_id}`, `_id` required;
- level < 4 → the `refprops` properties without `_id`;
- a reference within a reference keeps its own level; an array uses its item's;
- an array through an intermediate table is a list of references to the final
  model.

**Examples:** a schema carries one `example` (OpenAPI 3.0), and it satisfies
that schema. Identifiers are derived deterministically from the model name
(`_example_uuid`), so a regenerated file differs only where the manifest did; a
reference example is the identifier of the referenced model's example.

**Shared schemas** (`COMMON_SCHEMAS`) are included only when referenced: error
objects, `page`, `file`, `image`, `health`, `RateLimited`,
`UnpublishedReference` and others.

### Responses

| Code | When | Body |
|---|---|---|
| `200` | all operations | model, listing, property, `version`, `health`, token |
| `206`, `416` | `file`/`image` content | binary |
| `301` | single object | none; `Location` header |
| `304` | `get`, `head` | none; cache headers only |
| `400`, `401`, `403`, `404`, `500`, `503` | per operation | `{"errors": [...]}` |
| `429` | all operations | `RateLimited`, an open object (ADR-0001) |

**Errors:** an error object has five fields (`type`, `code`, `template`,
`context`, `message`) and nothing else. Each status code lists named errors
built from `spinta.exceptions` classes (`code` and `template` are an `enum` of
the class value); the last alternative is the generic `Error`, because Spinta
has more errors than can be listed and `authlib` errors carry only `code` and
`message`. The token endpoint's `400`/`401` is an OAuth 2.0 error (RFC 6749
5.2).

### Security

| Scheme | Type | Where |
|---|---|---|
| `UAPI_auth` | `oauth2` `clientCredentials` | data operations |
| `UAPI_client` | `http` `basic` | token endpoints |

- `tokenUrl` is `auth.token_url`, or the first server + `/:token`.
- `scopes` lists only those operations request.
- Operation scopes are built with the same `scope_formatter` Spinta authorizes
  with, from the model (property) and the action; every namespace above the
  model is given as an alternative.
- Listing: `:getall` or `:search` (alternatives); single object and property:
  `:getone`. `HEAD` is authorized as `GET`. `_page` without other parameters is
  authorized with `:getall`.
- Every authorized operation declares `401` and `403`.

## Regular expressions

Every `pattern` is written so that both ECMA 262 and RE2 (the gateway linter)
read it: **no lookaround** and **no repetition over 1000**. What a pattern
cannot say is given by `maxLength` or `not` beside it.

## Bounds

- **Request side:** everything is bounded, so the gateway rejects a bad request:
  `_limit`, `_select`, `_sort`, `_page`, `{id}`, `scope`, headers.
- **Response side:** only what Spinta builds itself is bounded (UUID, `base32`,
  page token); no bounds are guessed for data values.

## Cleanup

- Unused shared components (parameters, schemas, headers, responses) are
  removed, repeating over all kinds until nothing more is removed.
- Model schemas are always kept.

## Output

`write_openapi_manifest`: `.yml`/`.yaml` → YAML (no anchors or aliases),
otherwise JSON (`indent=2`, UTF-8); without `--output`, to stdout.

## Validity

Tests check that every generated specification:

- passes `openapi_spec_validator` OpenAPI 3.0 validation;
- has no pattern RE2 cannot read;
- has every `example` satisfy its schema (`OAS30Validator`);
- has no unused shared components;
- lists, on every operation, all parameters of its path;
- names errors that match `spinta.exceptions` classes.
