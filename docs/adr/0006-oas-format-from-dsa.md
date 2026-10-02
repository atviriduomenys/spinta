# ADR-0006: OAS `format` is taken from the DSA where the DSA states it

- **Status:** Accepted
- **Date:** 2026-10-02
- **Related:** [#2004](https://github.com/atviriduomenys/spinta/issues/2004),
  [PR #2012](https://github.com/atviriduomenys/spinta/pull/2012),
  [ADR-0005](0005-oas-integer-format-int64.md)

## Context

A `string` property of a model was described by its type alone. A gateway
validating responses could not tell an e-mail address or a web address from any
other text, and the lint reports asked for a `format`.

It was agreed that `format`, and a `pattern` where there is one, is added
wherever the DSA lets us know it. The DSA can say what a value is in two places:

- `property.type`: `url` and `uri` are types of their own;
- `property.uri`: a vocabulary term gives the meaning of the value, for example
  `vcard:hasEmail`.

`property.ref` was checked as well, against the DSA specification
([`tipai.rst`](https://github.com/ivpk/dsa/blob/main/dsa/tipai.rst)) and the
DSA files at hand. For primitive types it holds a unit (`integer`, `number`),
a currency (`money`), a precision (`date`, `datetime`, `time`, `geometry`) or a
text markup (`html`, `md`, `rst`, `tei` of a string with a language tag).

## Decision

1. `url` and `uri` properties are `format: uri`.
2. A `string` property gets a `format` from the term in its `uri`, after the
   prefix is expanded with the prefixes the DSA declares, or with the usual
   namespace of `vcard`, `foaf`, `schema` and `dcat` when it declares none:

   | Term | `format` |
   |---|---|
   | `vcard:hasEmail`, `vcard:email`, `schema:email` | `email` |
   | `foaf:homepage`, `foaf:page`, `schema:url`, `dcat:accessURL`, `dcat:downloadURL`, `dcat:landingPage` | `uri` |

   The example of such a property is one of that format.
3. Nothing is derived from `property.ref` of primitive types.

## Alternatives considered

1. **A `pattern` from the precision in `property.ref`** (`date` with `Y` as
   `YYYY-01-01`, for one). Spinta does not hold values to the precision, it
   serves them as the source gives them, so the pattern would refuse data the
   service answers with. Rejected.
2. **A phone number `pattern` from `vcard:tel`.** OpenAPI has no format for it,
   and the numbers in the data are written in many ways (`+370 …`, `8 …`).
   Rejected.
3. **`foaf:mbox` as `email`.** Its value is a `mailto:` URI, not an address.
   Left out.

## Consequences

- A gateway that enforces `format` refuses a response holding a value that is
  not one, an address without `@` in a field marked `vcard:hasEmail`, for one.
  The DSA is taken at its word: a wrong term is fixed in the DSA.
- A term not in the table above adds nothing. The table grows as DSA files use
  more terms.
