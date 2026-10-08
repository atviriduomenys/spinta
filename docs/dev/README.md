# Spinta developer documentation

Documentation for those who build and maintain Spinta: how it is structured and
how its features work. User documentation is in `docs/en` and `docs/lt`, and a
summary of changes in `CHANGES.rst`.

## What goes where

| Where | For | What |
|---|---|---|
| `CHANGES.rst` | users | briefly, what changed and what it means for using Spinta |
| `docs/en`, `docs/lt` | users | how to use Spinta: commands, configuration, examples |
| `docs/dev/architecture/` | developers | overall structure: components, how they relate, data flows |
| `docs/dev/specifications/` | developers | technical specification of one feature: what it does, from what and how |
| `docs/adr/` | developers | why a decision was made and which alternatives were weighed |

## Rules

- **A specification describes the final state, without history.** How it used
  to be and why it changed belongs in an ADR or the commit history. When
  behaviour changes, the specification is updated to describe what is.
- **One feature, one file** in `specifications/`, named after the feature
  (`udts-oas.md`).
- **A specification points at the code and tests** implementing the feature,
  and at the ADRs that shape it.
- **Do not repeat user documentation.** Link to `docs/en` or `docs/lt` instead.

## Contents

### Architecture

Not written yet, see [`architecture/README.md`](architecture/README.md).

### Specifications

| Specification | Feature |
|---|---|
| [UDTS data service OpenAPI specification](specifications/udts-oas.md) | `spinta udts oas`, `create_openapi_manifest` |
