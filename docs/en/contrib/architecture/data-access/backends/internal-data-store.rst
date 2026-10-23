.. default-role:: literal

###################
Internal data store
###################

Internal backends are where Spinta *stores* the data it manages itself:
data pulled from external sources, write requests coming through the API,
model schemas and changelogs (see
:doc:`/contrib/architecture/data-access/backends/index` for how backends are
registered and instantiated).

`postgresql` (`PostgreSQL`, `spinta/backends/postgresql/components.py`)
— the main production backend. Features: `WRITE`, `PAGINATION`, `EXPAND`,
`DISTRIBUTE` and `FILE_BLOCKS`. Per model it keeps a set of SQLAlchemy
tables (`backend.tables`), named with
`get_table_identifier()` (`spinta/backends/helpers.py`) and the
`TableType` enum from `spinta/backends/constants.py`: the main table
(`""`), the changelog (`/:changelog`), list tables of array properties
(`/:list`), file tables (`/:file`) and redirect tables (`/:redirect`).
Tables are created and upgraded by `commands.prepare` /
`commands.migrate` overloads (`spinta/backends/postgresql/commands/
init.py` and `migrate/`, `spinta migrate` in `spinta/cli/migrate.py`).
The PostgreSQL backend supports Citus distributing — models and config
carry a `DistributionStrategy` (see `model.distribution_strategy`,
`config.default_distribution_strategy`) implemented in
`spinta/backends/postgresql/helpers/migrate/citus.py` — and PostGIS
geometry storage (`spinta/backends/postgresql/types/geometry/`). Files
can be stored in the database in blocks (the `FILE_BLOCKS` feature, see
`spinta/backends/postgresql/types/file/`).

`memory` (`Memory`, `spinta/backends/memory/components.py`) — an
in-memory dict of tables by model name, with `WRITE` support. Used
almost exclusively in tests and for the default `memory` manifest.

`fs` (`FileSystem`, `spinta/backends/fs/components.py`) — stores `file`
properties on the file system under `backends.<name>.path` (default
environments configure `fs` alongside `postgresql`). On this backend the
`_id` of a file row is its path (see `spinta/commands/write.py` and
`spinta/types/file/helpers.py`).

`nobackend` (`NoBackend`, `spinta/backends/nobackend/components.py`) — a
placeholder assigned to external models without a source. It is not
registered in `components.backends` (it cannot be configured); read
commands raise `BackendNotGiven`. `NoBackend` is also used as the
backend value while manifest data is not reachable.

Transactions
************

Backends provide two context managers (implemented per backend):

- `begin()` — a plain working connection, used for reads;
- `transaction(write=False)` — a transaction; with `write=True` the
  PostgreSQL backend inserts a row into the `_txn` table and yields a
  `WriteTransaction` carrying the transaction id, which is recorded in
  the access log (`txn`) and attached to changed data.

Code never calls these directly: `validate_and_return_transaction()` and
`validate_and_return_begin()` (`spinta/backends/helpers.py`) first check
`backend.available` (polling with `commands.wait()` if needed) and then
produce the context manager. Callers attach the result to the request
context: writes attach `context.attach("transaction",
validate_and_return_transaction, ...)` (e.g. `spinta/utils/response.py`
before `commands.push`, `spinta/cli/data.py`, `spinta/cli/pull.py`),
while reads use `context.attach(f"transaction.{backend.name}",
validate_and_return_begin, ...)` so that several backends can be read in
parallel, and command overloads fetch the connection with
`context.get("transaction")` or `context.get(f"transaction.{backend.name}")`.

Other backend commands are dispatched per backend type, e.g.
`commands.wait(Context, PostgreSQL)` (`spinta/backends/postgresql/
commands/wait.py`), `commands.wipe`, `commands.changes`
(changelog reads, `spinta/backends/postgresql/commands/changes.py`),
write commands (`spinta/commands/write.py` dispatching to
`spinta/backends/postgresql/commands/write.py`, `spinta/backends/fs/
commands/write.py`, ...). A `Backend` advertises what it supports via
`BackendFeatures` and `backend.supports(feature)`; for example
pagination is enabled only when `BackendFeatures.PAGINATION` is
supported (`_link_model_page()` in `spinta/types/model.py`, see
:doc:`/contrib/architecture/data-access/pagination`).

Query and result builders
*************************

Each backend selects its ufunc environments with two string keys:

- `query_builder_type` — a key into `config.components['querybuilders']`
  (empty string `""` is the generic `QueryBuilder`); e.g. `postgresql`
  → `spinta.backends.postgresql.ufuncs.query.components:PgQueryBuilder`,
  `sql` → `SqlQueryBuilder`, `dask` → `DaskDataFrameQueryBuilder`;
- `result_builder_type` — a key into
  `config.components['resultbuilders']` (e.g. `postgresql` →
  `PgResultBuilder`, `sql` → `SqlResultBuilder`); empty `""` is the
  generic `ResultBuilder`.

The classes are looked up in `load_query_builder_class()` /
`load_result_builder_class()` (`spinta/backends/helpers.py`) right after
the backend is created and stored as `query_builder_class` /
`result_builder_class`. Read commands then use them through the ufunc
environment (see :doc:`/contrib/architecture/multiple-dispatch/ufuncs/index`): for `getall(Context, Model,
PostgreSQL)` (`spinta/backends/postgresql/commands/read.py`)::

   builder = backend.query_builder_class(context)
   builder.update(model=model)
   table = backend.get_table(model)
   env = builder.init(backend, table, params)
   expr = env.resolve(query)   # resolver phase
   where = env.execute(expr)   # executor phase
   qry = env.build(where)      # build the SQLAlchemy query
   result = conn.execute(qry)

and convert each row with the result builder (`get_row_value` /
`backend_result_builder_getter` from
`spinta/ufuncs/resultbuilder/helpers.py`). The external `Sql` backend
does the same in `spinta/datasets/backends/sql/commands/read.py`,
adding keymap-based `_id` encoding (see :doc:`/contrib/architecture/data-access/backends/external-data-sources`).


External specifications
***********************

- UAPI (https://github.com/atviriduomenys/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`): the spec is deliberately storage
  agnostic — it describes the JSON resources and actions (`getall`,
  `getone`, `push`, ...), not the backend. Spinta implements it by
  converting internal backend data (PostgreSQL rows) into UAPI
  responses.
- DCAT (https://www.w3.org/TR/vocab-dcat-3/, local extract
  `.tmp/sources/w3c/vocab-dcat-3.md`): an internal backend implements
  data *storage* — datasets and their distributions in Spinta terms
  (a `dcat:DataService` giving access to a `dcat:Distribution`).
- RML (https://rml.io/specs/rml/, local extract `.tmp/sources/rml/
  RML.md`): internal backends are the mapping *target* — the place
  where the mapped data lands, instead of RDF triples.

See also
********

- :doc:`/contrib/architecture/data-access/backends/backend` — the `Backend` base class.
- :doc:`/contrib/architecture/data-access/backends/external-data-sources` — the external source backends.
- :doc:`/contrib/architecture/multiple-dispatch/commands` — how backend behavior is
  implemented as dispatch overloads.
- `spinta/backends/constants.py` — `BackendFeatures`, `TableType`,
  `BackendOrigin`, `DistributionType`.
