.. default-role:: literal

#######
Request
#######

The `request` record is written when a data command starts serving a
request. It is emitted by the command overloads — e.g.
`commands.getone(Context, Request, Model, ...)` and
`commands.getall(...)` in `spinta/commands/read.py`,
`spinta/utils/response.py` (before dispatching writes),
`spinta/types/namespace.py` (namespace requests) and
`spinta/commands/write.py:log_request()` (write operations) — by calling
`accesslog.request(...)` on the attached access log:

.. code-block:: python

   accesslog = context.get("accesslog")
   accesslog.request(
       txn=str(uuid.uuid4()),
       model=model.model_type(),
       action=action.value,
   )

Record fields
*************

`AccessLog.request()` builds the JSON record (filtered down to the
non-`None` fields):

- always present — `time` (ISO-8601, recorded in UTC), `pid` (the web
  worker's process id), `type` (always ``request``), `txn`
  (a transaction id; for writes the actual backend write transaction,
  for reads a fresh UUID — reads have no write transaction,
  `commands/changes.py`), `method`, `action` (the `Action` value), and
  the request-scoped values loaded from the loaders — `format`, `url`,
  `client`, `token`, `agent` and, when `scope_log` is on, `scopes`;
- present depending on what is accessed — `rctype` (request body
  content type; write operations only), `ns` (accessed namespace),
  `model` (absolute model name), `prop` (accessed property subresource
  place), `id` (accessed object id), `rev` (accessed object revision,
  for direct object writes), `reason` (failure reason, e.g. for auth
  failures).

The start time and process memory are recorded at request time
(`time.monotonic()` and `psutil.Process().memory_info().rss`), so that
the `response` record can report diffs — see :doc:`response`.

Auth records
************

A third record type, `auth` (also implemented in
`spinta/accesslog/__init__.py:AccessLog.auth()`), is emitted on the
``/auth/token`` endpoint of the API (`spinta/api/__init__.py:
_auth_accesslog()`) when a token is issued — it logs `client`, `token`,
`scope` (when `scope_log` is on), `method`, `format` and `url`.

External specifications
***********************

- RFC 6749 (https://www.rfc-editor.org/info/rfc6749, local extract
  `.tmp/sources/ietf/RFC6749.md`): the record carries the OAuth client
  (`client`) and the issued token (`token`) — the client
  identification the resource server sees on every protected request
  (RFC 6749 section 1.1).
- UAPI contains no logging requirements; the fields (`ns`, `model`,
  `prop`, `id`, `rev`, `action`) mirror the UAPI URL structure served
  by the request.

See also
********

- :doc:`/manual/accesslog` — the full field reference and jq examples.
- :doc:`/contrib/architecture/access-log/index` — the access log
  overview and lifecycle.
