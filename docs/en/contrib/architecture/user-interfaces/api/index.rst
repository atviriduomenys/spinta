.. default-role:: literal

####
API
####

In C4 terms (see :doc:`/contrib/architecture/index`), the web app is one of Spinta's
:ref:`containers <contrib-containers>`; this page documents its
:ref:`components <contrib-components>`.

Spinta serves data over HTTP as a `Starlette <https://www.starlette.io/>`_
application. The app is created by `spinta/api/__init__.py:init()`, which
takes a fully loaded :doc:`Context </contrib/architecture/configuration/context>` (context, config,
store and manifest) and returns a Starlette `app` object. This page explains
how the app is run, which routes exist, how a request URL is parsed into
`UrlParams`, how the request is dispatched to data commands, and how backend
rows are converted into UAPI-compatible JSON. The authentication parts
(OAuth 2.0 token issuing and validation) are described in :doc:`/contrib/architecture/auth/index`.

Spinta implements the `UAPI`_ specification — it converts data stored in a
:class:`~spinta.backends.components.Backend` into UAPI-compatible JSON (see
`Conformance to UAPI`_ at the end of this page).


Two ways to run the server
**************************

The same Starlette app can be started in two ways:

- `spinta asgi` (or any ASGI server, e.g. `gunicorn spinta.asgi:app`) —
  imports `spinta/asgi.py`, which creates a module-level `context` with
  `spinta/core/context.py:create_context("asgi")` and runs the full startup
  pipeline (see :doc:`/contrib/architecture/wired-together`, section "How everything is wired together"):

  1. `commands.load(Context, Config)` and `commands.check(Context, Config)`;
  2. `commands.load(Context, Store)`;
  3. `commands.load(Context, Manifest)`, `commands.link()`,
     `commands.check()`;
  4. `commands.wait(Context, Store)` — wait for backends to become
     available;
  5. `commands.prepare(Context, Manifest)` — create database tables;
  6. `app = spinta.api.init(context)`.

  The `Context` used by the app is the module-level one;
  `ContextMiddleware` forks it per request (see below).

- `spinta run` (`spinta/cli/server.py:run()`) — the development server. The
  root CLI callback has already created a context in ``ctx.obj``
  (see :doc:`/contrib/architecture/user-interfaces/cli`); the handler then:

  1. converts manifest arguments with `convert_str_to_manifest_path()`;
  2. forks the context with `configure_context()`, adding the manifests,
     `mode` (`internal`/`external`), ``--backend`` and ``--backend_type``
     options;
  3. loads the store and manifest with `prepare_manifest()` (this includes
     `wait` and `prepare` steps);
  4. builds the app with `spinta.api.init(context)` and serves it with
     `uvicorn.run()` (default ``127.0.0.1:8000``).

  `spinta run` also sets ``AUTHLIB_INSECURE_TRANSPORT=1``, so that OAuth 2.0
  tokens work over plain HTTP in development.

`spinta wait` (`spinta/cli/server.py:wait()`) is a related helper: it loads
the store and calls `commands.wait(Context, Store)` without starting an HTTP
server — used to check from scripts that all backends are up.


The Starlette app
*****************

`spinta/api/__init__.py:init()` builds the `Starlette` app:

Routes
======

Fixed routes (registered in `init()`):

.. list-table::
   :header-rows: 1

   * - Route
     - Methods
     - Handler
   * - `/robots.txt`
     - `GET`
     - `spinta/api/__init__.py:robots()`
   * - `/favicon.ico`
     - `GET`
     - returns an empty 404 response
   * - `/version`
     - `GET`
     - `commands.get_version()`
   * - `/health`
     - `GET`
     - `spinta/api/health.py:health()`
   * - `/auth/token`
     - `POST`
     - OAuth 2.0 token endpoint (see :doc:`/contrib/architecture/auth/index`)
   * - `/auth/introspect`
     - `POST`
     - token introspection (RFC 7662)
   * - `/.well-known/jwks.json`
     - `GET`
     - `get_verification_keys()` — public token signing keys
   * - `/.well-known/oauth-authorization-server`
     - `GET`
     - authorization server metadata (RFC 8414)
   * - `/_srid/{srid}/{x}/{y}`
     - `GET`
     - `srid_check()` — converts coordinates to an OSM link
   * - `/auth/clients`
     - `GET`, `POST`
     - client list / client creation
   * - `/auth/clients/{client}`
     - `GET`, `DELETE`, `PATCH`
     - client read / delete / update
   * - `/{path:path}`
     - `HEAD`, `GET`, `POST`, `PUT`, `PATCH`, `DELETE`
     - `homepage()` — all data requests

If `config.docs_path` is set, static docs are also mounted at `/docs`.

The catch-all route `/{path:path}` must be registered last, because it
matches everything. All data requests go through `spinta/api/__init__.py:
homepage()`.

`/health` is a synchronous handler run by Starlette in a worker thread. It
reports `{"healthy": ..., "dependencies": [...]}` where the dependencies are
`spinta`, `disk` (free space at `config.data_path`) and `memory`. Details of
a failed check go to the log, never to the response — the probe is not
authenticated. Configuration options are documented in
:doc:`/manual/configuration/health`.


Middleware
==========

Middleware is added in this order:

1. `StrictTransportSecurityMiddleware` (`spinta/middlewares.py`) — adds the
   `Strict-Transport-Security` header to every response (value from
   `config.http_strict_transport_security`).
2. `PathNormalizationMiddleware` — *rejects* requests whose path is not in
   normalized form (dot segments, percent-encoded slashes, duplicate slashes)
   with a 404 and ``Cache-Control: no-store`` response. This prevents web
   cache poisoning: shared caches normalize URLs when building cache keys,
   while the application would otherwise see the original path.
3. `ContextMiddleware` — forks the global, preloaded `Context` for each
   request and puts the fork into `request.state.context`. Handlers then use
   `context.set()`, `context.bind()` and `context.attach()` to add
   request-specific state (`auth.token`, `accesslog`, `transaction`, ...).
   Fork/bind/attach semantics are described in `HACKING.rst` (section
   "Context") and summarized in :doc:`/contrib/architecture/configuration/context`.


Error handling
==============

All exceptions are handled by `spinta/api/__init__.py:error()`, which renders
every error as::

    {"errors": [{"code": "...", "message": "..."}]}

- `BaseError` and `MultipleErrors` (Spinta exceptions) use their own status
  code, headers and error details (`error_response()`).
- `AuthlibHTTPError` (authlib OAuth errors) adds a
  ``WWW-Authenticate: Bearer error="..."`` header.
- Anything else becomes a 500 with the exception message.
- Error responses always get ``Cache-Control: no-store`` (they may reflect
  the request path and must never be cached).
- If the negotiated format is `html`, the error is rendered with the
  `error.html` template instead of JSON.


Authentication setup
====================

`init()` also binds the auth components into the context:

- ``auth.server`` — `spinta/auth.py:AuthorizationServer`;
- ``auth.resource_protector`` — `spinta/auth.py:ResourceProtector` with a
  `BearerTokenValidator`.

Both are used by the `/auth/*` routes and by every data request
(see :doc:`/contrib/architecture/auth/index`).


Request handling flow
*********************

`homepage()` is the handler for all data requests:

1. The request is turned into an authlib request
   (`get_auth_request(StarletteOAuth2Data(...))`) and the access token is
   validated; the token is stored as ``auth.token`` (see :doc:`/contrib/architecture/auth/index`).
2. A per-request manifest is created (`commands.create_request_manifest()`,
   `commands.load_for_request()`, `commands.reload_backend_metadata()`) —
   the manifest is needed to resolve models for the given URL.
3. `prepare(Context, UrlParams(), Version(), request)` (registered in
   `spinta/urlparams.py`) parses the URL into `UrlParams`.
4. An `AccessLog` entry is attached with loaders for the store, token,
   request and params (see :doc:`/contrib/architecture/auth/index`, section on accesslog).
5. `spinta/utils/response.py:create_http_response()` dispatches to a data
   command based on `params.action` and the request method, and returns the
   rendered response.


UrlParams: parsing the request URL
**********************************

`UrlParams` (`spinta/components.py`) holds everything parsed from the
request: path parts, model, primary key, property, format, `select`/`sort`/
`limit`/`offset`, query expressions, pagination, language preferences and the
resolved `Action`.

`spinta/urlparams.py:prepare()` builds `params.parsetree` from the URL path
(`spinta/utils/url.py:parse_url_path()`) and the query string
(`parse_url_query()` — the query string is parsed as a spyna expression, see
:doc:`/contrib/architecture/multiple-dispatch/ufuncs/index`). The parsetree is then split by
`_prepare_urlparams_from_path()` into path operators.

Path operators
==============

The recognized path operators (from `spinta/utils/url.py:RULES` and
`spinta/urlparams.py:_prepare_urlparams_from_path()`):

.. list-table::
   :header-rows: 1

   * - Operator
     - Effect
   * - `/{Namespace}/{Model}/{id}/{prop}`
     - the path itself: namespace, model (starts with an uppercase
       letter), object id, property subresource
   * - `:ns`
     - the path is a namespace, list model names only
   * - `:all`
     - recursively include all models under a namespace
   * - `:changes`
     - changelog instead of data
   * - `:select(prop, ...)`
     - property selection
   * - `:sort(prop, ...)`
     - sorting
   * - `:limit(n[, offset])`
     - page size and offset
   * - `:offset(n)`
     - offset
   * - `:count()`
     - adds `count()` to `select` (backwards compatible)
   * - `:format(name, params)`
     - response format and its parameters
   * - `:summary`
     - batch summary response
   * - `:bbox(x1, y1, x2, y2)`
     - bounding box filter (geometry)
   * - `:page(key, size(n))`
     - keyset pagination (see :doc:`/contrib/architecture/data-access/pagination`)
   * - `:expand(prop, ...)`
     - expand references
   * - `:lang(...)`
     - language selection
   * - `:custom-scope(name)`
     - activate a custom authorization scope
   * - `:schema`
     - schema action (see below)
   * - `:inspect`
     - inspect action (see below)
   * - `:move`
     - move action
   * - `:wipe`
     - wipe action
   * - `:check`
     - check action
   * - `:fault-tolerant`
     - continue batch requests after errors

Any other path operator is treated as a query expression and collected into
`params.query` (e.g. ``/Model?eq(name, 'foo')``).

Actions
=======

`get_action()` resolves the `Action` (`spinta/core/enums.py`) from the
method and the parsed parameters:

- `POST` -> `insert`, `PUT` -> `update`, `PATCH` -> `patch`,
  `DELETE` -> `delete` (`METHOD_TO_ACTION`);
- `GET`/`HEAD`: `changes` if `:changes` was given, `getone` if an object id
  is present, otherwise `search` when any of `select`/`sort`/`limit`/
  `offset`/query is present, and `getall` when the request is a plain
  listing;
- `:schema`, `:inspect`, `:move`, `:wipe`, `:check` set their action
  explicitly.

Path resolution
===============

`_resolve_path()` walks the path parts:

- the model name starts at the first part beginning with an uppercase letter
  (`_find_model_name_index()`) — everything before it is the namespace path;
- the model is looked up with `commands.has_model()` /
  `commands.get_model()`, or as a namespace with
  `commands.has_namespace()` (`:ns` requires a namespace);
- the next part is the object id (`is_object_id()` validates it; a leading
  ``=`` is stripped);
- the next part is a property subresource (must be in
  `model.flatprops`; a ``:ref`` suffix sets `params.propref` to access the
  reference content instead of the referenced value);
- anything after that raises `ModelNotFound`.

Content negotiation
===================

The response format is taken from the `:format(...)` operator, or negotiated
from the ``Accept`` header by `get_response_type()`: each exporter's
`accept_types` are matched against the header media types (`text/html` and
`application/xhtml+xml` always map to `html`). The format name is resolved
into a `Format` instance from `config.exporters` and stored as `params.fmt`.

Language preferences are parsed from the ``Accept-Language`` and
``Content-Language`` headers (`parse_accept_lang_header()`) or taken from
the `:lang(...)` operator.

Custom scopes
=============

If the model has custom scopes (see :doc:`/contrib/architecture/auth/index`) and the request activates
one via `:custom-scope(name)`, `_apply_custom_scope()` restricts the request:
the scope's `prepare` formula contributes extra query filters and, if it has
a `select(...)`, the allowed field set — selecting, sorting or filtering by
fields outside it raises an error.


Dispatching to data commands
****************************

`create_http_response()` (`spinta/utils/response.py`) maps the action and
method to data commands:

.. list-table::
   :header-rows: 1

   * - Request
     - Command
   * - `GET` + `:changes`
     - `commands.changes()`
   * - `GET` + object id
     - `commands.getone()`
   * - `GET` + `:summary`
     - `commands.summary()`
   * - `GET` (otherwise)
     - `commands.getall()`
   * - `POST` + `:inspect`
     - `spinta/api/inspect.py:inspect_api()`
   * - `POST` + `:schema`
     - `spinta/api/schema.py:schema_api()`
   * - `DELETE` + `:wipe`
     - `commands.wipe()` (write transaction)
   * - `DELETE` + `:move`
     - `commands.move()`
   * - `POST`/`PUT`/`PATCH`/`DELETE` (otherwise)
     - `commands.push()` (write transaction); a property subresource pushes
       to the `params.prop.dtype` backend, with `:ref` to the property
       reference backend
   * - `:check`
     - `_check()` — manifest check, optionally with an uploaded manifest
       file (`POST` form field `manifest`)

Before dispatching, `create_http_response()` attaches request-scoped
context state:

- read requests attach a read transaction via
  `validate_and_return_transaction()`; `getall` additionally attaches
  ``transaction.{backend.name}`` via `validate_and_return_begin()` (this
  also waits until the backend is available);
- if the model has a keymap, a copy is attached as ``keymap.{name}`` so
  that ids can be decoded (see :doc:`/contrib/architecture/keymap/index`);
- ``cache-control`` is bound with `cache_control_response_headers()`: based
  on the latest changelog entry, responses get ``ETag`` (the `_revision`
  value), ``Last-Modified`` and ``Vary: Accept, Accept-Language,
  Authorization`` headers; ``If-None-Match``/``If-Modified-Since`` requests
  are answered with 304.

For non-streamable formats (`Format.streamable` is `False` only for `html`),
`_enforce_limit()` caps the response at `limit_enforced_to` (100) rows even
if the client did not ask for a limit.


From backend rows to UAPI JSON
******************************

All read commands follow the same pattern — this is where `Backend` data is
converted into UAPI-compatible JSON. Take `commands.getall(Context, Model,
Request)` in `spinta/commands/read.py`:

1. `commands.authorize()` checks the token scopes against the model
   (see :doc:`/contrib/architecture/auth/index`).
2. The `UrlParams` are converted into a query expression with
   `urlparams_to_expr()` and executed against the backend
   (`commands.getall(Context, Model, Backend)` or, when pagination is
   enabled, `commands.getall(Context, Model, Page)` — see
   :doc:`/contrib/architecture/data-access/pagination`). The backend yields rows in its own
   representation (e.g. SQLAlchemy rows for PostgreSQL).
3. `prepare_data_for_response()` (in `spinta/commands/read.py`) trims the
   rows to the selected properties and adds reserved properties:
   `_type`, `_id` and `_revision` (plus `_base` for `search` and `_page`
   when pagination is enabled). It dispatches to
   `commands.prepare_data_for_response(Context, Model, Format, dict)` overloads
   (generic ones in `spinta/backends/__init__.py`) which walk the property
   tree (`select_model_props()`) and call
   `commands.prepare_dtype_for_response()` per property — backend-specific
   overloads convert backend values into JSON-serializable primitives
   (e.g. `UUID` and `Decimal` for the `json` format, geometry values for
   PostgreSQL). Function properties from `select(count())` and friends are
   handled separately via `prepare_dtype_for_response()` on their dtypes.
4. `log_response()` records the response in the accesslog.
5. `spinta/renderer.py:render()` calls `commands.render(Context, Request,
   Node, Format)` — format-specific overloads in `spinta/formats/*/`
   build the final Starlette response.

For the `json` format (`spinta/formats/json/commands.py`):

- `getall`, `search` and `changes` return a streaming response. The stream
  is a JSON object ``{"_data":[...rows...]}`` and, when pagination returned
  a next-page key, ends with ``"_page": {"next": "..."}`` (see
  :doc:`/contrib/architecture/data-access/pagination`).
- `getone` and writes return a single JSON object via `JSONResponse`.
- `delete` returns an empty response.

Error responses are always
``{"errors": [{"code": ..., "message": ...}]}`` (see `Error handling`_).


Exporters
*********

Formats are registered in `config.exporters`
(`spinta/config.py:CONFIG`) with implementations in `spinta/formats/`:

.. list-table::
   :header-rows: 1

   * - Name
     - Content type
     - Implementation
   * - `json`
     - `application/json`
     - `spinta/formats/json/`
   * - `jsonl`
     - `application/x-json-stream`
     - `spinta/formats/jsonlines/`
   * - `csv`
     - `text/csv`
     - `spinta/formats/csv/`
   * - `xlsx`
     - `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`
     - `spinta/formats/xlsx/`
   * - `html`
     - `text/html`
     - `spinta/formats/html/`
   * - `rdf`
     - `application/rdf+xml`
     - `spinta/formats/rdf/`
   * - `ascii`
     - `text/plain`
     - `spinta/formats/ascii/`

Each format is a `Format` subclass (`spinta/formats/components.py`) with
`content_type`, `accept_types`, `params`, `streamable` (only `html` is not
streamable) and `prioritize_uri` (`rdf`). Format implementations register
`commands.render()` overloads and `prepare_dtype_for_response()` overloads
for value conversion specific to the format.


Inspect and schema endpoints
****************************

Two write-side actions operate on schemas rather than data:

- `POST .../:inspect` (`spinta/api/inspect.py:inspect_api()`, scope
  `uapi:/...:inspect`) — inspects an external data source given as form
  fields (`dataset`, `resource.source`/`resource.file`, `resource.type`,
  `resource.prepare`, optionally an explicit `manifest.*`) and returns a
  manifest skeleton (csv by default), created by
  `create_manifest_from_inspect()`. This is the HTTP counterpart of
  ``spinta inspect`` (see :doc:`/contrib/architecture/user-interfaces/cli`).
- `POST .../:schema` (`spinta/api/schema.py:schema_api()`, scope
  `uapi:/...:schema_write`) — applies a new dataset schema (an uploaded csv
  manifest) to a dynamic manifest: validates it, computes renames with
  `create_migrate_rename_mapping()` and runs `commands.migrate()` so the
  backend schema is updated without wiping data.


Conformance to UAPI
*******************

Spinta implements the UAPI specification (an extract is available in
`.tmp/sources/uapi/UAPI.md`). What current code serves:

- URL structure is ``/{namespace}/{Model}[/{id}][/{property}]`` with path
  operators (see `Path operators`_) — namespaces and models are resolved
  from the manifest, and model names are distinguished from namespace names
  by case (models start with an uppercase letter).
- Reserved object properties `_type`, `_id`, `_revision` (plus `_page` for
  paginated responses and `_base` for `search`) are added by
  `prepare_data_for_response()`.
- Data is returned as UAPI-compatible JSON: `getall`/`search`/`changes`
  stream ``{"_data": [...]}``, `getone` returns a single object.
- Actions are derived from the HTTP method and path operators, and
  authorization uses `uapi:/...` scopes (see :doc:`/contrib/architecture/auth/index`).

.. NOTE::

   The UAPI 2.0 draft URL structure — mandatory `{form}/{publisher}/{is}/
   {service}/{version}` segments, `@scope` path segments and reserved query
   parameters starting with ``_`` (`_select`, `_sort`, ...) — is **not**
   implemented by current code. Current Spinta takes operators as path
   operators and spyna expressions in the query string. Spyna itself — the
   expression language used to parse the query string — is documented on the
   child page below.

.. _UAPI: https://ivpk.github.io/uapi

.. toctree::
   :maxdepth: 1

   Spyna <spyna>


See also
********

- :doc:`/contrib/architecture/auth/index` — OAuth 2.0 token issuing, scopes and authorization.
- :doc:`/contrib/architecture/user-interfaces/cli` — `spinta run` and the other CLI commands.
- :doc:`/contrib/architecture/multiple-dispatch/commands` — how the dispatched data commands work.
- :doc:`/contrib/architecture/data-access/pagination` — keyset pagination.
- :doc:`/contrib/architecture/configuration/context` — context fork/bind/attach semantics.
- :doc:`/contrib/architecture/data-access/backends/index` — internal and external backends that the API reads from.
- :doc:`spyna` — the spyna expression language used to parse the query string.
