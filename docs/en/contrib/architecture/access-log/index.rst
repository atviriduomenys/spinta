.. default-role:: literal

##########
Access log
##########

The access log tracks which API endpoints are accessed. For each request
up to two JSONL records are created — one `request` record and,
optionally, one `response` record (skipped when the request failed
before a response was produced). Both records share the same `txn`
transaction id. The user-facing documentation (configuration, field
reference, jq recipes) is in :doc:`/manual/accesslog` — this section
describes the implementation in code.

Implementation
**************

Access log lives in `spinta/accesslog/`:

- `spinta/accesslog/__init__.py` — the `AccessLog` base class with the
  record builders (`request()`, `response()`, `auth()`), the
  `load(Context, AccessLog, ...)` command overloads and the
  `create_accesslog()` / `log_response()` helpers;
- `spinta/accesslog/file.py` — `FileAccessLog`, the production
  component: appends JSON Lines to a file (`accesslog.file` config
  option; `stdout`, `stderr`, `null` or a path);
- `spinta/accesslog/python.py` — `PythonAccessLog`, used in tests: every
  record is JSON-serialized and appended to an in-memory list shared via
  the context (`context.set("accesslog.stream", [])` in the session
  fixture, `spinta/testing/pytest.py`).

The component class is selected by `accesslog.type` (from the
`components.accesslog` registry, `spinta/config.py:CONFIG`) and
instantiated in `load(Context, Store)`
(`spinta/types/store.py:store.accesslog = commands.load(context,
config.AccessLog(), config)`).

Lifecycle
*********

The access log is bound per request with a context attach —
`context.attach("accesslog", create_accesslog, context, loaders=...)`
(from `spinta/utils/response.py` before dispatching to data commands;
see :doc:`/contrib/architecture/wired-together`). The loaders populate
the log entry with request-scoped state: `load(Context, AccessLog,
Token)` sets `client` and `token` (the JWT `jti`; scopes are recorded
only when `scope_log` is on), `load(Context, AccessLog, Request)` sets
`method`, `url`, `content_type` (`rctype`) and `agent`, and
`load(Context, AccessLog, UrlParams)` sets the response `format`.
`load(Context, AccessLog, Store)` opens the target file for
`FileAccessLog`.

Data commands then emit the records (see :doc:`request` and
:doc:`response`).

External specifications
***********************

- RFC 6749 (https://www.rfc-editor.org/info/rfc6749, local extract
  `.tmp/sources/ietf/RFC6749.md`): logging access to protected
  resources is the resource server's duty (section 1.1); the per-request
  entry carries the client and the token id (`jti`), which is the
  OAuth client identifier.
- DSA / UAPI have no access-log requirements — the access log is a
  Spinta-specific concern.

See also
********

- :doc:`/manual/accesslog` — configuration and log message format
  reference.
- :doc:`/contrib/architecture/configuration/store` — where the access
  log instance is stored (`store.accesslog`).
- `spinta/accesslog/` — the implementation.

.. toctree::
   :maxdepth: 1

   Request <request>
   Response <response>
