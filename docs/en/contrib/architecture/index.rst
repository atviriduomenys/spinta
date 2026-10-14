.. default-role:: literal

############
Architecture
############

Spinta docs (this section) are organized according to the `C4 model`_
abstraction levels — `software system` -> `containers` (applications and
data stores) -> `components` (modules within a container) -> `code`
(classes and other code elements).

.. _C4 model: https://c4model.com/
.. _DSA: https://dsa.readthedocs.io/
.. _DSA specification: https://ivpk.github.io/dsa/1.1/
.. _UAPI specification: https://ivpk.github.io/uapi/
.. _Conceptual model: https://raw.githubusercontent.com/atviriduomenys/
   katalogas/f8ad4b7d9f332ed9f9ada6c4e3e735512745e328/uml.drawio.svg

.. _contrib-system-context:

System context
**************

At the highest level Spinta is a software system for data
management and exchange.
It reads metadata — descriptions of datasets and models — from manifests
and uses it to work with data in two modes:

1. **External mode** — data is read from external data sources and
   served via the HTTP API in real time, without storing it in any
   intermediate storage.
2. **Internal mode** — data is stored in the internal data store managed
   by Spinta (where it can also be versioned). The internal data store is
   typically populated from external data stores with the
   ``spinta push`` command.

In both modes data is served back over the HTTP API. The HTTP API is
compatible with the `UAPI specification`_ and manifests are compatible
with the `DSA specification`_. Metadata model is DCAT-compatible at the
dataset level. Data can be pulled from external data sources, and pushed
to other Spinta instances.

People who use Spinta:

- **Data consumers** — read open data over the HTTP API (JSON, CSV, RDF,
  XLSX); starting points are :doc:`user-interfaces/api/index` and
  :doc:`data-access/index`.
- **Data publishers** — describe datasets and models in manifest files and
  load or publish data with the `spinta` command line tool (see
  :doc:`user-interfaces/cli` and :doc:`manifests/index`).
- **Operators** — deploy Spinta and configure backends, auth clients and
  the access log (see :doc:`configuration/index` and :doc:`auth/index`).

External systems Spinta talks to:

- **External data sources** — SQL servers, CSV/JSON/XML files read with
  dask, WSDL/SOAP services; see
  :doc:`data-access/backends/external-data-sources`.
- **Other (remote) Spinta instances** — data can be pulled from and pushed
  to them, and keymaps can be synchronized via
  ``{server}/{model}/:changes`` (see
  :doc:`/contrib/architecture/data-access/changes`).
- **Metadata Catalogue** — Spinta instances can synchronize metadata (manifests)
  with a central Metadata Catalogue using the :doc:`sync` command (see also
  the architecture issue `#katalogas/2302`_).
- **Authorization server** — publish JWKS public keys used to validate
  access tokens (see :doc:`auth/index`). Both an external authorization
  server (e.g. Keycloak) and Spinta's built-in authorization server
  (used for testing) can be used.

.. _#katalogas/2302: https://github.com/atviriduomenys/katalogas/issues/2302

.. _contrib-containers:

Containers
**********

Container is a C4 Model architecture element — independently runnable applications and data stores. Each container is documented in detail at the following pages:

Applications
============

For each application (separate process started by a `spinta` CLI command), see
its dedicated documentation:

- **Agent** — ``spinta run --mode=external``. See :doc:`user-interfaces/api/index` and :doc:`data-access/backends/external-data-sources`.

- **Saugykla** (Lithuanian for "repository") — ``spinta run
  --mode=internal``. See :doc:`user-interfaces/api/index` and
  :doc:`data-access/backends/internal-data-store`.

- **Data sync** — ``spinta push`` (and ``spinta pull``). See
  :doc:`data-access/backends/internal-data-store`.

- **Metadata sync** — ``spinta sync``. See :doc:`sync`.

- **Manifest management tools** — ``spinta inspect`` and ``spinta copy``. See
  :doc:`manifests/index`.

- **Keymap sync** — ``spinta keymap sync``. See :doc:`keymap/index`.

- **Data migrations** — ``spinta bootstrap`` and ``spinta migrate``. See
  :doc:`data-access/backends/internal-data-store`.

- **Import/export tool** — ``spinta import`` and ``spinta export``. See
  :doc:`user-interfaces/cli`.

Data stores
===========

For each data store (persistent or temporary storage), see its dedicated
documentation page:

- **Internal data store** — PostgreSQL (including Citus and PostGIS),
  fs, memory, or nobackend. See :doc:`data-access/backends/internal-data-store`.

- **External data store** — Spinta's read drivers for external sources.
  See :doc:`data-access/backends/external-data-sources`.

- **Keymap store** — SQLite (via SQLAlchemy) or Redis. See :doc:`keymap/index`.

- **Access log** — JSONL files, stdout, or Python logging. See
  :doc:`access-log/index`.

- **Manifest files** — CSV, ASCII, XLSX, YAML, JSON, XML on disk or
  URLs. See :doc:`manifests/index`.

Components
**********

Code is organized in the following major parts. They map onto the
containers above the following way: all applications — the Agent and
Saugykla servers, the push, sync, keymap, migrate, import/export and
inspect tools — share the cross-cutting layers (core, manifests,
datasets, types, commands, ufuncs) and differ only in their entry
components.

Cross-cutting core
==================

`spinta/core/context.py`
    `create_context()` — the single entry point to create a `Context`,
    plus `load_commands()`, which imports all command and ufunc modules
    listed in `CONFIG['commands']['modules']` and `CONFIG['ufuncs']`.
    See :doc:`context`.

`spinta/core/config.py` + `spinta/config.py`
    Configuration: `RawConfig` reader with `ConfigSource` subclasses
    (`PyDict`, `Path`, `CliArgs`, `EnvVars`, `EnvFile`), and the default
    `CONFIG` dictionary, which also registers components (manifest
    formats, backends, components types, keymaps, exporters, etc.).
    See :doc:`configuration/index`.

`spinta/core/ufuncs.py`, `spinta/spyna.py`, `spinta/ufuncs/`
    Expression language: spyna AST parser (`spinta.spyna.parse()`),
    `Expr` tree, `Env` interpreter base class, `ufunc.resolver`,
    `ufunc.executor` registries, `asttoexpr()`. See
    :doc:`multiple-dispatch/index` and :doc:`user-interfaces/api/spyna`.

`spinta/components.py`
    Core component classes: `Context`, `Store`, `Component`, `Node`,
    `MetaData`, `ExtraMetaData`, `Namespace`, `Model`, `Base`, `Property`,
    `Config`, `Command`, `UrlParams`, `Page`, `Version`, `DataItem`, etc.

`spinta/types/`
    Data types — `DataType` subclasses (e.g. `PrimaryKey`, `String`,
    `Object`, `Array`, `Ref`, `File`, `Geometry`) plus their per-type
    command overloads; type classes are registered in
    `spinta/config.py:CONFIG['components']['types']`. See
    :doc:`manifests/logical/data-type`.

`spinta/commands/` (the commands registry) and `spinta/dispatcher.py`
    Command definitions: `@command()` decorated stubs in
    `spinta/commands/__init__.py`, and `Command` (multiple dispatcher)
    machinery from `spinta/dispatcher.py`. See
    :doc:`multiple-dispatch/commands`.

`spinta/nodes.py`
    Node instantiation helpers, `spinta/nodes.py:get_node()` gives you
    the right node class per manifest type.

`spinta/utils/`
    Generic Python utilities: HTTP helpers (`spinta/utils/http.py`),
    response helpers (`spinta/utils/response.py`, e.g.
    `create_http_response()`, data helpers
    (`spinta/utils/data.py`), validators, timezone helpers, etc.

`spinta/naming/`
    Model and property name helpers (e.g. names for `ns`).

Manifests — shared by the web app and the CLI
=============================================

`spinta/manifests/`
    Manifest implementations, one per format, all extending
    `spinta.manifests.components.Manifest`: `tabular` (`csv`, `ascii`,
    `xlsx`, `gsheets`), `yaml` (plus `inline`), `dict` (`json`, `xml`),
    `memory` (in-memory, used in tests), `backend` (stored in an internal
    backend), `internal_sql` (shared in an internal PostgreSQL backend,
    used for partial management and migrations), `sql`, `rdf`, `xsd`,
    `xsd2`, `open_api`, `mermaid`. See :doc:`manifests/index`.

`spinta/dimensions/`
    Additional manifest dimensions beyond `dataset`/`resource`/`model`/
    `property`: `enum`, `lang`, `comments`, `prefix`, `scope`, `param`.
    These extend the tabular/yaml manifests.

HTTP API (Agent and Saugykla)
=============================

`spinta/api/`, `spinta/asgi.py`
    HTTP API (Starlette application): `spinta/api/__init__.py:init()`
    creates routes and middleware; `spinta/asgi.py` exposes module-level
    Starlette `app` served by e.g. uvicorn (same as `spinta run`). See
    :doc:`user-interfaces/api/index`.

`spinta/auth.py`, `spinta/accesslog/`, `spinta/middlewares.py`
    OAuth 2.0 (`RFC 6749`_) server parts (`AuthorizationServer`,
    `ResourceProtector`, `BearerTokenValidator`, `Token`, `Client`,
    `Scopes`, `get_scope_name`, `authorized`), access log
    (`spinta/accesslog/file.py:FileAccessLog`,
    `spinta/accesslog/python.py:PythonAccessLog`), and middlewares
    (`ContextMiddleware` forks the context per request,
    `PathNormalizationMiddleware`,
    `StrictTransportSecurityMiddleware`). See :doc:`auth/index` and
    :doc:`access-log/index`.

`spinta/formats/`, `spinta/urlparams.py`, `spinta/renderer.py`
    Response formatting (`Format` implementations — `json`, `jsonl`,
    `csv`, `html`, `rdf`, `xlsx`, `ascii`), URL parsing into `UrlParams`
    (`spinta/urlparams.py:prepare()`), and response rendering
    (`spinta/renderer.py:render()`).

Command line tools
==================

`spinta/cli/` and `spinta/cli/main.py` (`spinta` `Typer` app)
    CLI handlers: `config.py`, `auth.py` (`key`, `token`, `client`),
    `manifest.py` (`copy`), `inspect.py`, `data.py` (`import`, `export`),
    `pull.py`, `push.py`, `server.py` (`run`, `wait`), `migrate.py`
    (`bootstrap`, `sync`, `migrate`), `keymap.py`, `show.py`,
    `uncomment.py`, `comment.py`, `admin.py`, `upgrade.py`, `pii.py`.
    See :doc:`user-interfaces/cli`.

Internal backend
================

`spinta/backends/`
    Internal backends, where model definitions and data are stored:
    `postgresql` (main production backend; also supports Citus
    distribution and PostGIS geometry), `fs` (file system), `memory`
    (in-memory, used in tests), `nobackend`. See
    :doc:`data-access/backends/internal-data-store`.

`spinta/datasets/keymaps/`
    KeyMap implementations: `spinta/datasets/keymaps/sqlalchemy.py
    SqlAlchemyKeyMap` (default; sqlite or another SQLAlchemy storage)
    and `spinta/datasets/keymaps/redis.py RedisKeyMap`. See
    :doc:`keymap/index`.

External data stores
====================

`spinta/datasets/`
    External data source descriptions and loaders: `Dataset`,
    `Resource`, `Entity`, `Attribute` ("dataset" node classes) in
    `components.py`, plus `spinta/datasets/backends/`, which implement
    drivers for external data sources. See
    :doc:`manifests/logical/index`,
    :doc:`manifests/physical/index` and
    :doc:`data-access/backends/external-data-sources`.

Also check the `tests/` directory — there are many manifest examples and
test cases illustrating specific features.
See :ref:`writing-tests`.

.. _RFC 6749: https://www.rfc-editor.org/info/rfc6749

.. toctree::
   :titlesonly:
   :maxdepth: 3

   UML diagram <uml-diagram>
   C4 architecture diagram <c4-diagrams>
   How everything is wired together <wired-together>
   Configuration <configuration/index>
   Multiple dispatch <multiple-dispatch/index>
   User interfaces <user-interfaces/index>
   Manifests <manifests/index>
   Data access <data-access/index>
   Auth <auth/index>
   Access log <access-log/index>
   Keymap <keymap/index>
   Synchronization <sync>
   Changes <data-access/changes>
