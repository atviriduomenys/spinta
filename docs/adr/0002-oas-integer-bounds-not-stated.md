# ADR-0002: OAS integer properties carry no `format` or bounds

- **Status:** Accepted
- **Date:** 2026-09-17
- **Related:** [#2004](https://github.com/atviriduomenys/spinta/issues/2004),
  [PR #2012](https://github.com/atviriduomenys/spinta/pull/2012)

## Context

The gateway team lints generated data service specifications with `vacuum` and
the gateway rule set. Two OWASP rules in it ask of every `integer` schema:

- `owasp-integer-format`: `format: int32` or `format: int64`;
- `owasp-integer-limit`: `minimum` and `maximum` (or `exclusiveMinimum` and
  `exclusiveMaximum`).

In real data service specifications these two rules are the only `vacuum`
errors left. All of them are on response schemas of model properties whose DSA
type is `integer`, for example:

```json
"duration": {"type": "integer", "nullable": true}
```

Spinta does not know the range of these values. DSA `integer` has no bounds,
and a UDTS agent reads the data from an institution's source, whose column
width (16, 32, 64 bits or unbounded) the manifest does not describe. The
possible values are a property of the source data, not of Spinta.

On the request side bounds are already given, because there they are a gateway
policy: everything a client sends is bounded, so that the gateway rejects a bad
request before it reaches the service. `_limit` has `minimum: 1`, a `maximum`
from `limits.max_limit` and a `format`, and an integer `{id}` path parameter
has `int64` bounds.

## Decision

Integer schemas of model properties get **no** `format`, `minimum` or `maximum`,
and `owasp-integer-format` and `owasp-integer-limit` are **not fixed** for them.

The specification states only what Spinta knows: the value is an integer, and,
where the property is not required, it may be `null`.

## Alternatives considered

1. **Write `format: int64` and 64-bit bounds everywhere.** The findings would go
   away, but the specification would claim what Spinta does not know: a source
   column may be narrower or wider (`NUMERIC` without precision, for example).
   That works around the rule rather than answering it: the bounds would
   protect nothing, since nobody set them, and gateway response validation would
   reject values of a column wider than 64 bits. Rejected.
2. **Write `format: int32` and its bounds.** The same, and riskier: common
   64-bit identifiers and counters would be rejected. Rejected.
3. **Let DSA state the bounds.** The accurate solution, but the DSA
   specification has no column for it. That is a question for the DSA
   specification, not for the OAS generator. Out of scope.

## Consequences

- Until DSA can state value bounds, `vacuum` with the gateway rule set reports
  `owasp-integer-format` and `owasp-integer-limit` for every specification with
  integer properties. The gateway team treats them as known and accepted, for
  example by ignoring them or turning these rules off for response schemas.
- Request-side bounds stay (`_limit`, integer `{id}`), because there they are a
  gateway policy. The `int64` bound of `{id}` assumes the key fits in 64 bits; an
  object with a wider source key could not be reached through the gateway.
  These OWASP rules are meant for bounding requests.
- If DSA gains value bounds, this ADR is superseded and the generator takes the
  bounds from the manifest.
