# Architecture Decision Records

This directory records architecture decisions: what was decided, why, and what
follows from it. An ADR is written when a decision is not obvious from the code,
when several alternatives were weighed, or when someone, a person or a reviewer,
may later propose to change it.

## Rules

- One decision per file, `NNNN-short-title.md`.
- Numbered in order from `0001`; a number is never reused.
- An accepted ADR is not changed in substance. When a decision changes, a new
  ADR is written and the old one is marked "Superseded by ADR-NNNN".
- Statuses: **Proposed**, **Accepted**, **Rejected**, **Superseded by ADR-NNNN**.

A new ADR follows the structure of the existing ones: *Context*, *Decision*,
*Alternatives considered*, *Consequences*.

An ADR explains **why** something was decided. **How** a feature works is
described in the developer documentation, [`docs/dev`](../dev/README.md).

## Index

| No. | Title | Status | Date |
|---|---|---|---|
| [0001](0001-oas-429-open-object.md) | OAS `429` response is an open object; rate limits are applied by the gateway | Accepted | 2026-09-17 |
| [0002](0002-oas-integer-bounds-not-stated.md) | OAS integer properties carry no `format` or bounds | Accepted | 2026-09-17 |
| [0003](0003-oas-version-3-0.md) | Data service specification is written as OpenAPI 3.0 | Accepted | 2026-09-15 |
| [0004](0004-oas-empty-identifier-not-described.md) | An empty declared identifier is not described in OAS | Accepted | 2026-09-17 |
