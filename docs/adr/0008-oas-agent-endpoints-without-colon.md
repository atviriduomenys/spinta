# ADR-0008: Agent endpoints are described as `/version`, `/health` and `/token`

- **Status:** Accepted
- **Date:** 2026-10-13
- **Supersedes:** [ADR-0007](0007-oas-agent-endpoints-not-described.md)
- **Related:** [#2004](https://github.com/atviriduomenys/spinta/issues/2004),
  [PR #2012](https://github.com/atviriduomenys/spinta/pull/2012)

## Context

ADR-0007 left the agent endpoints out of the generated document, because the
gateway routes them by hand with Dynamic Routing rules. Colleagues asked for
them back: whoever reads the document, in the gateway's developer portal or
elsewhere, should find the version, the health probe and the token endpoint of
the data service next to its data.

Before ADR-0007 they were described in two forms: `/:version`, `/:health` and
`/:token` as the gateway routes them, and `/version`, `/health` and
`/auth/token` as the agent serves them, told apart by `x-spinta-context`.

## Decision

It was agreed with colleagues that the agent endpoints are **described again,
as they were described in the gateway form, but without the colon**:
`/version`, `/health` and `/token`. They are relative to the document's
`servers`, like the data paths, and their operations, schemas and responses
are the ones used before ADR-0007.

Only a data service export carries them. A whole manifest export has no data
service base for the gateway to route them under.

`tokenUrl` of `UAPI_auth` defaults to the first server and `/token`.

## Alternatives considered

1. **Keep them out (ADR-0007).** Rejected at the colleagues' request.
2. **Describe both forms again, marked by `x-spinta-context`.** The agent form
   is of use only to a client calling the agent directly, and the extension was
   one of the remarks on the earlier documents. Rejected.
3. **Keep the colon, `/:version`.** Rejected at the colleagues' request.

## Consequences

- `/version`, `/health` and `/token`, the `utility` tag, the `UAPI_client`
  scheme and the `version`, `health`, `token` and OAuth 2.0 error schemas are
  back in a data service document.
- The gateway still needs Dynamic Routing rules for these paths, now matched
  without the colon, since the agent serves them at its root and the token
  endpoint as `/auth/token`.
- A model of the data service named `version`, `health` or `token` at the root
  of the service would take one of these paths.
