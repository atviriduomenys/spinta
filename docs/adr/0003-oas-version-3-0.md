# ADR-0003: Data service specification is written as OpenAPI 3.0

- **Status:** Accepted
- **Date:** 2026-09-15
- **Related:** [#2004](https://github.com/atviriduomenys/spinta/issues/2004),
  [PR #2012](https://github.com/atviriduomenys/spinta/pull/2012)

## Context

`spinta udts oas` and `create_openapi_manifest` generate the OpenAPI
specification of a data service. The generator wrote OpenAPI `3.1.0`.

- OpenAPI `3.0` is the version named in legislation and other requirements for
  data services, so this is not only a technical choice.
- Colleagues reported that generated files would not import into the API
  gateway (Gravitee). Gravitee documents import support for Swagger 2.0 and
  OpenAPI 3.0.

OpenAPI 3.1 and 3.0 schemas differ in substance: the 3.1 Schema Object is JSON
Schema 2020-12, the 3.0 one an adapted subset of it. Changing the version number
alone is not enough.

## Decision

The specification is written as **OpenAPI `3.0.3`** and uses nothing 3.0 does
not have:

| 3.1 | 3.0 |
|---|---|
| `"type": ["string", "null"]` | `"type": "string", "nullable": true` |
| a reference that may be `null` | `anyOf: [{$ref}, {"type": "object", "nullable": true, "enum": [null]}]` |
| `example` beside `$ref` | `allOf: [{$ref}]` with `example` beside it (3.0 ignores siblings of `$ref`) |
| a schema `examples` list | a single `example` |
| `const` | `enum` of one value |
| `info.summary` | first paragraph of `info.description` |
| `info.license.identifier` | not written; left out of the configuration with a warning |

Every `pattern` is also written so that RE2 reads it, since the gateway linter
applies RE2 to a 3.0 document: no lookaround and no repetition over 1000. What a
pattern cannot say is given by `maxLength` or `not` beside it.

## Alternatives considered

1. **Keep 3.1.** Does not meet the requirements and does not import into the
   gateway. Rejected.
2. **Generate either version by a parameter.** Twice the code and tests, while
   nobody would use a 3.1 file as long as the requirements name 3.0. Rejected.
3. **Generate 3.1 and convert it with an external tool.** The conversion is not
   unambiguous (nullable references, `examples`), and Spinta's tests would not
   validate its result. Rejected.

## Consequences

- The specification imports into the gateway and passes `openapi_spec_validator`
  OpenAPI 3.0 validation; tests also check that every `example` satisfies its
  schema under OpenAPI 3.0 rules (`OAS30Validator`).
- Tests validate schemas with an OpenAPI 3.0 validator rather than JSON Schema,
  which does not understand `nullable`.
- Swagger UI no longer warns about a deprecated schema `example`; in 3.0 it is
  a regular field.
- 3.1 constructs are not brought back into the generator. If the requirements
  ever name 3.1, a new ADR is written.
