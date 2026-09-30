# ADR-0005: OAS integers are `int64`, still without bounds

- **Status:** Accepted
- **Date:** 2026-09-30
- **Supersedes:** [ADR-0002](0002-oas-integer-bounds-not-stated.md)
- **Related:** [#2004](https://github.com/atviriduomenys/spinta/issues/2004),
  [PR #2012](https://github.com/atviriduomenys/spinta/pull/2012)

## Context

[ADR-0002](0002-oas-integer-bounds-not-stated.md) left integer schemas of model
properties without `format`, `minimum` and `maximum`, because DSA `integer`
states no width and Spinta does not know the range of the source data. As a
result, `vacuum` with the gateway rule set reports two OWASP rules for every
integer property of a data service:

- `owasp-integer-format`: `format: int32` or `format: int64`;
- `owasp-integer-limit`: `minimum` and `maximum`.

These were the only errors left in real data service specifications.

## Decision

The team agreed on 2026-09-30 that **every** `integer` schema in the generated
document carries `format: int64`: model properties, array items, `_limit`
(until now `int32` when `limits.max_limit` fit into it) and the other integer
fields.

Bounds stay as ADR-0002 set them: model properties get no `minimum` or
`maximum`, and request-side bounds (`_limit`, an integer `{id}`) stay.

## Alternatives considered

1. **Keep ADR-0002, no `format`.** Accurate, but `owasp-integer-format` stays
   in every lint report of a data service. Rejected.
2. **`int64` with 64-bit bounds as well.** It would also silence
   `owasp-integer-limit`, but the bounds would restate the width and protect
   nothing, since nobody chose them. Not decided; `owasp-integer-limit` stays.
3. **`int32` where values are small.** Spinta does not know which values are
   small. Rejected.

## Consequences

- `owasp-integer-format` no longer fires. `owasp-integer-limit` still fires for
  integer properties of the data, as ADR-0002 describes.
- The document claims a 64-bit width that DSA does not state. A validator that
  enforces `int64` may refuse a response holding a value of a wider source
  column (`NUMERIC` without precision, for example). This was accepted as
  unlikely for the data served.
- `_limit` is described as `int64` whatever `limits.max_limit` is.
- If DSA gains a width or bounds for `integer`, this ADR is superseded and the
  generator takes them from the manifest.
