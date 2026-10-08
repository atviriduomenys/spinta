# ADR-0007: Agent endpoints are not described in the data service OAS

- **Status:** Accepted
- **Date:** 2026-10-02
- **Related:** [#2004](https://github.com/atviriduomenys/spinta/issues/2004),
  [PR #2012](https://github.com/atviriduomenys/spinta/pull/2012)

## Context

Besides the data, a UDTS agent serves endpoints of its own at its root:
`/version`, `/health` and the token endpoint `/auth/token`. Inside a data
service the API gateway reaches them through Dynamic Routing rules, as
`/:version`, `/:health` and `/:token`; everything else is routed to the data
path.

The generated document described both forms, the gateway one and the agent
one, marked by `x-spinta-context`. The gateway imports endpoints from the
document, so the agent endpoints came in through the import as well, while the
rules that route them still had to be set up by hand.

## Decision

It was agreed with colleagues that agent endpoints are **not described**
in the generated document. The gateway adds them by hand with Dynamic Routing
rules.

`components.securitySchemes.UAPI_auth` still gives `tokenUrl`, because the
OAuth 2.0 flow is not described without it. It is `auth.token_url`, or the
first server and `/:token`, the gateway rule.

## Alternatives considered

1. **Describe both forms, marked by `x-spinta-context`.** What the document did
   until now. A gateway importing it has to drop the paths it routes itself,
   and the agent form is of use only to a client calling the agent directly.
   Rejected.
2. **Describe the gateway form only.** The routes would be imported, but the
   rules behind them are set up by hand anyway, and the list of agent endpoints
   changes with the authorization server (Keycloak is to replace Spinta there).
   Rejected.

## Consequences

- `/version`, `/health`, `/auth/token`, their gateway forms, the `utility` tag,
  the `UAPI_client` scheme and the schemas only they used (`version`,
  `health`, `token`, OAuth 2.0 errors) are gone from the document.
- `x-spinta-context` is gone with them.
- A client calling the agent directly, from Postman for one, finds these
  endpoints in the agent documentation rather than in this document.
