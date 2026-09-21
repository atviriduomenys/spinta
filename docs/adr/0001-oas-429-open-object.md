# ADR-0001: OAS `429` response is an open object; rate limits are applied by the gateway

- **Status:** Accepted
- **Date:** 2026-09-17
- **Related:** [#2004](https://github.com/atviriduomenys/spinta/issues/2004),
  [PR #2012](https://github.com/atviriduomenys/spinta/pull/2012)

## Context

`spinta udts oas` generates the OpenAPI specification of a data service. The API
gateway uses it to import endpoints and to validate requests and responses (the
*OpenAPI Specification Validation* policy in the Request and Response phases).

Every operation declares a `429 Too Many Requests` response. Spinta has no rate
limits of its own and never answers `429`; whatever stands in front of it does.
So the question is what the specification says about the `429` body:

- At first the `RateLimited` schema had no `type`, so that any body would pass
  (an object, text or nothing), since it was not known who applies the limit.
- The gateway team, linting the files with `vacuum` and the gateway rule set,
  got `oas-missing-type` and asked for `type: object`.
- A Copilot review pointed out that `type: object` rejects a non-JSON `429`
  body if a proxy between the gateway and Spinta applies the limit (nginx
  answers with HTML by default): to the gateway such an answer looks like one
  of the service and goes through response validation.

The risk exists only when something **between the gateway and Spinta** limits
requests. When the gateway limits them, it answers with a JSON object; a WAF in
front of the gateway never reaches the gateway's validation.

## Decision

The `RateLimited` schema of the `429` response is an **open object**:

```json
"RateLimited": {
  "type": "object",
  "description": "Answer of a rate limit reached. …",
  "properties": {},
  "example": {"message": "Rate limit exceeded", "http_status_code": 429}
}
```

- `type: object`: the gateway answers a rate limit with a JSON object;
- no required fields and `properties: {}`: the gateway, not Spinta, decides the
  fields, so validation does not reject a `429` over its content;
- the media type stays `*/*`.

This rests on the agreement with partners that **rate limits are applied
through the gateway**. Partners should not rate limit in front of the agent, or,
if they do, not more strictly than the gateway, so that the gateway is always
the one answering `429`.

## Alternatives considered

1. **Leave the `429` body undescribed (no `type`).** Accepts any body, but the
   gateway linter reports it (`oas-missing-type`), and with limits applied by
   the gateway the flexibility is not needed. Rejected.
2. **Describe the gateway's answer exactly** (required `message`,
   `http_status_code`). Ties the specification to one gateway version and its
   answer format. Rejected.
3. **Change the media type to `application/json`.** More precise, but changes
   nothing for validation while the body is an object, and is stricter still in
   the proxy case. Not done.

## Consequences

- The specification passes the gateway `vacuum` rule set without
  `oas-missing-type`.
- If a partner still applies a stricter limit in a proxy between the gateway
  and Spinta and it answers with a non-JSON body, gateway response validation
  rejects that `429` and the client gets a gateway error instead. That breaks
  the agreement and is settled with the partner, not in the specification.
- A reviewer's suggestion to make the `429` body undescribed again is declined
  with a reference to this ADR.
- Implemented in `COMMON_SCHEMAS["RateLimited"]`,
  `spinta/manifests/open_api/openapi_config.py`.
