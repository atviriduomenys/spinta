:orphan:

.. default-role:: literal

*******
Backend
*******

`spinta/backends/components.py:Backend` is the base class of *all*
backends — internal storage backends (`spinta/backends/`) and external
source backends (`spinta/datasets/backends/`, see
:doc:`external-backend`). A backend instance is a *connection* to one
storage or one source, named and configured in the `backends`
configuration section (see :doc:`/contrib/architecture/data-access/backends/index`).

Note that `Backend` extends `Component` (see :doc:`/contrib/architecture/concepts/component`) and has a
`metadata` schema declaring the backend's configuration keys — for
`PostgreSQL` this is::

   metadata = {
       "name": "backend",
       "properties": {"dsn": {"type": "string", "required": True}},
   }

Attributes
**********

- `type` — the backend type name as in `config.components['backends']`
  (e.g. `"postgresql"`, `"sql/postgresql"`).
- `name` — the backend instance name (the key in the `backends`
  configuration section, e.g. `"default"`).
- `origin` — a `BackendOrigin` enum value (`config`, `manifest`,
  `resource`) saying where the backend definition came from (see
  :doc:`/contrib/architecture/data-access/backends/index`).
- `config` — the original configuration values of the backend (used to
  render manifests back to their original form).
- `available` — a flag set to `False` when a connection fails; readers
  first poll the backend with `commands.wait()` (see
  `spinta/backends/helpers.py:validate_and_return_transaction()`).
- `features` — a set of `BackendFeatures` (`spinta/backends/
  constants.py`): `WRITE`, `PAGINATION`, `EXPAND`, `DISTRIBUTE`,
  `FILE_BLOCKS`. Checked with `supports(feature)` — e.g. pagination is
  only enabled for backends with `PAGINATION` (see :doc:`/contrib/architecture/data-access/pagination`).
- `query_builder_type` and `result_builder_type` — keys into
  `config.components['querybuilders']` / `['resultbuilders']`; resolved
  to classes (`query_builder_class`, `result_builder_class`) at load
  time by `spinta/backends/helpers.py:load_query_builder_class()`.
- `model_requires_source` — whether models must declare a source (table
  or object name) to use this backend; `False` on `Memory`,
  `FileSystem` and on `dask/csv`/`dask/memory` external backends.

Distribution is not a backend attribute: it is per *model*
(`model.distribution_strategy` — a `DistributionStrategy` dataclass
(`spinta/backends/components.py`) with a `DistributionType` —
`schema`, `table`, `copy`, `undistributed` — and an optional sharding
`property`), applied to PostgreSQL tables when running on Citus (see
`spinta/backends/postgresql/helpers/migrate/citus.py`).

Methods
*******

- `transaction(write=False)` — context manager yielding a transaction
  object holding the connection (for `PostgreSQL` a `ReadTransaction` /
  `WriteTransaction` with a transaction `id` from the `_txn` table).
  Writes use `write=True`.
- `begin()` — a plain connection context manager for reads.
- `bootstrapped()` — whether the backend is already initialized (tables
  created).
- `supports(feature)` — `feature in self.features`.

Commands
**********

Backend behavior is implemented as `@commands.<name>.register()`
overloads dispatched on the backend class (multiple dispatch, see
:doc:`/contrib/architecture/multiple-dispatch/commands`). Two overloads are always needed:

- `load(Context, Backend, dict)` — connect the backend
  (`spinta/backends/postgresql/commands/load.py` builds the engine);
- `wait(Context, Backend)` — poll availability
  (`spinta/backends/postgresql/commands/wait.py`).

Then, per backend: `prepare` / `migrate` (table creation, e.g.
`spinta/backends/postgresql/commands/init.py`), `getall`/`getone`
(reads, dispatched as `(Context, Model, PostgreSQL)`),
`getall(Context, Model, Sql)` for external sources, `write`/`push`,
`wipe`, `changes`, `bootstrap` and so on.

The full backend type list is configured in
`spinta/config.py:CONFIG['components']['backends']`; instantiation is
done by `load_backend()` in `spinta/backends/helpers.py` during
`load(Context, Store)` (see :doc:`/contrib/architecture/data-access/backends/index`, :doc:`/contrib/architecture/configuration/store`).

External specifications
***********************

- DCAT (https://www.w3.org/TR/vocab-dcat-3/, local extract
  `.tmp/sources/w3c/vocab-dcat-3.md`): a concrete backend instance is
  the Spinta counterpart of a `dcat:DataService` — a service through
  which datasets and their distributions are accessed.
- UAPI (https://github.com/atviriduomenys/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`): the UAPI protocol is backend-agnostic;
  `Backend` is the layer that makes a storage qualify as a UAPI resource
  server (actions are dispatched onto backend-specific command
  overloads).
- RFC 6749 (https://www.rfc-editor.org/info/rfc6749, local extract
  `.tmp/sources/ietf/RFC6749.md`): the backends together implement the
  state of the resource server (sections 1.1, 2.1); backend availability
  and transactions are what the API serves behind OAuth protected
  requests.

See also
********

- :doc:`/contrib/architecture/data-access/backends/index` — the backends module overview.
- :doc:`external-backend` — the external source backend base class.
- :doc:`/contrib/architecture/manifests/logical/models` — where `model.backend` is resolved.
- `spinta/backends/constants.py` — `BackendFeatures`, `TableType`,
  `BackendOrigin`.
