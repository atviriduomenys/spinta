:orphan:

.. default-role:: literal

***************
ExternalBackend
***************

`spinta/datasets/components.py:ExternalBackend` is the base class of
external source backends — read-only drivers that read data from
external databases, files and APIs. It extends `Backend` (see
:doc:`backend`, the internal side is described in
:doc:`/contrib/architecture/data-access/backends/index`).

External backends are registered in the same
`config.components['backends']` registry as internal backends, but they
are only used for reading: they allow `model_requires_source = True`
(the model must point to a table or object in the source) and they are
resolved per `Resource` (see `load_resource_backend()` in
`spinta/datasets/helpers.py` and :doc:`/contrib/architecture/data-access/backends/external-data-sources`).

Attributes
**********

- `engine` (`sqlalchemy.engine.Engine`) and `schema`
  (`sqlalchemy.MetaData`) — the SQLAlchemy connection and reflected
  schema, filled by per-backend `load` overloads (e.g.
  `spinta/datasets/backends/sql/commands/load.py` connects using the
  resource `dsn`).
- `model_requires_source = True` — models backed by an external source
  must have a source name (`Entity.name`, from the `pk`/`source`
  manifest columns). Exceptions: `dask/csv` and `dask/memory` set this
  to `False` — CSV files and memory buffers are usually the single
  table of their resource.

(Note: `ExternalBackend` adds these attributes on top of `Backend`; it
has no `features` of its own except what subclasses declare — e.g.
`Sql` sets `BackendFeatures.PAGINATION`.)

Transactions
************

External backends never write: they are paired with an internal
backend that stores pulled data.

- `transaction()` raises `NotImplementedError` (writing happens into
  the *internal* backend — e.g. `spinta pull` reads from the external
  backend and writes to `manifest.backend`), while
- `begin()` simply yields (or opens the connection and stores it on the
  backend, like `WsdlBackend.begin()` building a `zeep` client).
  Reads attach it as `context.attach(f"transaction.{backend.name}",
  validate_and_return_begin, ...)` — see :doc:`/contrib/architecture/data-access/backends/index`.

Registered types
****************

All registered in `spinta/config.py:CONFIG['components']['backends']`:

.. list-table::
   :header-rows: 1

   * - Type
     - Class / module
     - Notes
   * - `sql`
     - `Sql` (`spinta/datasets/backends/sql/components.py`)
     - Generic SQL reader with `PAGINATION`; tables are reflected via
       `Sql.get_table()`; read commands in
       `spinta/datasets/backends/sql/commands/read.py` with
       `SqlQueryBuilder` (`sql`) and `SqlResultBuilder`.
   * - `sql/sqlite`, `sql/postgresql`, `sql/mssql`, `sql/mysql`,
       `sql/mariadb`, `sql/oracle`, `sql/sas`
     - `Sqlite`, `PostgreSQL`, `MSSQL`, `MySQL`, `MariaDB`, `Oracle`,
       `SAS` in `spinta/datasets/backends/sql/backends/<name>/`
     - Per-dialect `Sql` subclasses, each picking its own query builder
       (`query_builder_type = "sql/<name>"`, e.g.
       `SqliteQueryBuilder`, `OracleQueryBuilder`, `SASQueryBuilder` in
       `.../ufuncs/query/components.py`).
   * - `dask`
     - `DaskBackend` (`spinta/datasets/backends/dataframe/components.py`)
     - Base class of the dataframe backends: data is read into a Dask
       DataFrame and queried with `DaskDataFrameQueryBuilder`.
   * - `dask/csv`, `dask/json`, `dask/xml`, `dask/memory`
     - `spinta/datasets/backends/dataframe/backends/<name>/`
     - Format readers; `dask/csv` and `dask/memory` allow models
       without a source name (`model_requires_source = False`).
   * - `soap`
     - `Soap` (`spinta/datasets/backends/dataframe/backends/soap/`)
     - Calls a SOAP operation (`service`, `port`, `operation`) through
       a `WsdlBackend` client; has its own `SoapQueryBuilder`.
   * - `wsdl`
     - `WsdlBackend` (`spinta/datasets/backends/wsdl/components.py`)
     - Holds the `zeep` client built from the WSDL URL given as the
       resource `dsn` (raises `WsdlClientError` on failure).
   * - `sqldump`
     - `SqlDump` (`spinta/datasets/backends/sqldump/components.py`)
     - Reads SQL dumps from a file path or stdin.
   * - `xlsx`, `geojson`, `html`
     - `BackendNotImplemented` (`spinta/datasets/backends/notimpl/`)
     - Registered placeholders with no implementation.
   * - `csv`, `xml`, `json`
     - `spinta/compat.py` (`CsvDeprecated`, ...)
     - Deprecated pre-dask implementations, kept for backwards
       compatibility.

Note the name clash trap: `sql/postgresql` (external,
`spinta/datasets/backends/`) is a different class from `postgresql`
(internal, `spinta/backends/`).

External specifications
***********************

- RML (https://rml.io/specs/rml/, local extract `.tmp/sources/rml/
  RML.md`): an external backend with its resource `dsn`/`type` plays the
  role of RML's *logical source* (`rml:source` +
  `rml:referenceFormulation`) — it both locates the data and states how
  it is serialized.
- DCAT (https://www.w3.org/TR/vocab-dcat-3/, local extract
  `.tmp/sources/w3c/vocab-dcat-3.md`): the external backend is the
  Spinta counterpart of a `dcat:DataService`; the resource `dsn`
  (URL/DSN/file path) is the `dcat:Distribution` `dcat:downloadURL` /
  `dcat:accessURL`.
- UAPI (https://github.com/atviriduomenys/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`): UAPI is a data exchange protocol over
  HTTP; external source backends never serve UAPI directly — their data
  is pulled into the internal backend first, and it is the internal
  backend that serves UAPI resources.

See also
********

- :doc:`/contrib/architecture/data-access/backends/index` — internal vs external backends overview.
- :doc:`backend` — the `Backend` base class.
- :doc:`/contrib/architecture/manifests/physical/resources` — the `Resource` node that carries
  `resource.backend`.
- :doc:`/contrib/architecture/data-access/backends/external-data-sources` — resource backend resolution and keymap.
