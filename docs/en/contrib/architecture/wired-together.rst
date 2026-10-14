.. default-role:: literal

How everything is wired together
################################

Startup
=======

Both CLI and server use the same single startup pipeline (see
`spinta/asgi.py`):

1. `spinta/core/context.py:create_context()` — reads configuration with
   `spinta/core/config.py:read_config()` via `RawConfig` (sources
   `Path(spinta.config)` -> `EnvFile` -> `EnvVars` -> `CliArgs`; in that
   order, last has highest precedence), `Config` object (a python object
   with `spinta.components:Config` as its class) and instantiates
   `Store`.
2. All command and ufunc handler modules get imported via
   `spinta/core/context.py:load_commands()` from modules listed in
   `spinta/config.py:CONFIG['commands']['modules']`. That is how command
   overloads registered `@commands.<name>.register(...)` decorators get
   registered.
3. `commands.load(Context, RConfig)` — split into two phases: first
   `commands.load(Context, Config)` (`spinta/types/config.py`) builds the
   `Config` component from raw values (`config.components`,
   `config.backends`, `config.manifests`, `config.keymaps`,
   `config.exporters`, `config.AccessLog` and other options), then
   `commands.load(Context, Store)` (`spinta/types/store.py`) creates
   all internal backends and manifests declared in the configuration via
   `rc.get('manifests')` — the main manifest is stored as
   `store.manifest`, the import one is stored as `store.internal`.
4. Then `commands.load(Context, Manifest)` reads manifest nodes (e.g. csv
   rows or YAML files) into `manifest._objects`. This is implemented in
   `spinta/manifests/<type>/commands/*` modules.
5. `commands.link(Context, Store.manifest)` — resolves references in
   manifest nodes which aren't known at load time, e.g. `Model.ns`
   namespace tree from model name, or `Property.dtype` (e.g. some types
   like `ref` and `backref` are linked with other properties or models);
   see `spinta/commands/link.py` and type-aware backends overrides.
6. `commands.check(Context, Store.manifest)` — validates manifest
   definitions (identifiers, types, access levels etc.).
7. `commands.wait(Context, Store)` — tells Spinta to wait for the
   databases to become available (e.g. PostgreSQL connections are up).
8. `commands.prepare(Context, Store.manifest)` — prepare run-time state
   (e.g. for the internal PostgreSQL backend, tables — model tables and
   `internal` extra tables like `_schema` and `_txn` — are created here;
   see `spinta/backends/postgresql/commands/init.py`, where the
   `post_init` overload creating internal and model tables lives).

At this point a `Starlette` app is created via
`spinta/api/__init__.py:init()` (described in
:doc:`/contrib/architecture/user-interfaces/api/index`) and either
run with `uvicorn` (dev: `spinta run` in `spinta/cli/server.py`) or
served e.g. via `gunicorn` upstream `spinta.asgi`.

.. _API:

How a server handles requests
=============================

Each request first goes through middlewares (see
`spinta/middlewares.py`), then `spinta/api/__init__.py` routes:

- `StrictTransportSecurityMiddleware` — adds HSTS headers.
- `PathNormalizationMiddleware` — rejects non-normalized request paths
  (dot segments, duplicate slashes, percent-encoded slashes/backslashes)
  with a 404 response that cannot be cached (`Cache-Control: no-store`) —
  a web cache poisoning defense (see `spinta/middlewares.py`).
- `ContextMiddleware` — forks the global `Context` per request and puts
  the fork in `request.state.context` (see `HACKING.rst` on
  `fork()`/`bind()` context semantics).
- `spinta/api/homepage()` — the main treated route handler for the
  `/{path:path}` route: OAuth access token is parsed into
  `StarletteOAuth2Data` (`spinta/auth.py:get_auth_request()`
  produces a `StarletteOAuth2Request`),
  authorized client is set in `context.get('auth.token')`; then URL is
  parsed into `UrlParams` (see `spinta/urlparams.py:prepare()`), then
  the matching data command is called, based on `params.action` —
  one of `commands.getone` (`getone`), `commands.getall` (`getall`,
  `search`), `commands.changes` (`changes`), `commands.push` (`insert`,
  `upsert`, `update`, `patch`, `delete`); requests are dispatched through
  `spinta/utils/response.py:create_http_response()`; requests are rendered
  by `commands.render()` using exporters in `spinta/formats/` (see
  `config.exporters`), via `spinta/formats/*/` implementations.

Other fixed endpoints are (in `spinta/api/__init__.py:init()`):
`/version`, `/health`, plus `/auth/token` (OAuth 2.0 client credentials
token generation, via authorization server), `/auth/introspect`
(introspection endpoint), `/auth/clients` client CRUD endpoints, and
`/.well-known/jwks.json` (server public key), plus
`/.well-known/oauth-authorization-server` (discovery). The full request flow is described in
:doc:`/contrib/architecture/user-interfaces/api/index`.



See also
********

- :doc:`/contrib/architecture/user-interfaces/cli` — how CLI handlers
  drive the same pipeline (`load_config` → `load_store` →
  `load_manifest` → `prepare_manifest`).
- :doc:`/contrib/architecture/configuration/index` — how configuration
  is assembled at step 1.
- :doc:`/reference/commands` — commands reference, including
  startup flow.
- :doc:`/hacking` — internals work log, with the `Context`
  (fork/bind/attach) semantics described in detail.
