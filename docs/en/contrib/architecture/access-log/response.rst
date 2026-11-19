.. default-role:: literal

########
Response
########

The `response` record is written when a data command finishes producing
a response. It is emitted from two places:

- `getone` — `accesslog.response(objects=1)` right after the object is
  fetched (`spinta/commands/read.py`);
- streaming commands (`getall`, `search`, `changes`) — the
  `log_response()` / `log_async_response()` generators
  (`spinta/accesslog/__init__.py`) count the yielded rows and call
  `accesslog.response(objects=objects)` after the last one; the count is
  therefore available only after the client has received the whole
  response.

Record fields
*************

`AccessLog.response()` builds the JSON record:

- `time` — ISO-8601, recorded in UTC;
- `type` — always ``response``;
- `txn` — the transaction id of the matching request record;
- `delta` — seconds between request and response
  (`time.monotonic()` diff);
- `memory` — RAM used by the web worker while the request was in
  flight (`psutil` RSS diff);
- `objects` — number of *top level* objects processed (composite
  objects attached to a top level object are not counted).

The response record might be missing when the request failed before any
response was produced — error paths raise before the generator finishes,
so only requests that actually stream or return data get a response
entry.

Error paths
***********

Failed requests do not get a response record: the error propagates
through `spinta/api/__init__.py:error()`, which renders the
``{"errors": [...]}`` JSON, without touching the access log — the
`request` record (`reason` field, filled by the exception handling in
`spinta/api/__init__.py`) stays the evidence that something failed.

External specifications
***********************

- UAPI (https://github.com/atviriduomenys/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`) has no logging requirements — `objects`
  counts the UAPI *resources* of the response: one for `getone`
  endpoints, otherwise the number of streamed rows (the `_data` array
  length of a UAPI response).
- RFC 6749 (https://www.rfc-editor.org/info/rfc6749, local extract
  `.tmp/sources/ietf/RFC6749.md`): responses to protected resource
  requests are what the resource server serves (section 1.1) — the
  access log records that the protected response was served with in the
  shared `txn` value.

See also
********

- :doc:`/manual/accesslog` — the full field reference and jq examples.
- :doc:`request` — the matching request record.
- :doc:`/contrib/architecture/access-log/index` — the access log
  overview.
