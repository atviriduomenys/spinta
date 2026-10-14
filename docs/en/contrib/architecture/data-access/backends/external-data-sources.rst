.. default-role:: literal

#####################
External data sources
#####################

External source backends are *read-only* drivers that read data from
external databases, files and APIs. They extend `ExternalBackend`
(see :doc:`/contrib/architecture/data-access/backends/external-backend`), which adds
a SQLAlchemy `engine` and `schema` to the `Backend` base and a
`model_requires_source` flag — the model must point to a table or
object in the source (exceptions: `dask/csv` and `dask/memory`).

Registered types
****************

Note the name clash trap: `sql/postgresql` (external,
`spinta/datasets/backends/`) is a different class from `postgresql`
(internal, `spinta/backends/`).

.. list-table::
   :header-rows: 1

   * - Type
     - Class
     - Notes
   * - `sql`
     - `Sql` (spinta/datasets/backends/sql/components.py)
     - Generic SQLAlchemy-based reader with `BackendFeatures.PAGINATION`
       (`SqlQueryBuilder`, `SqlResultBuilder`).
   * - `sql/sqlite`, `sql/postgresql`, `sql/mssql`, `sql/mysql`,
       `sql/mariadb`, `sql/oracle`, `sql/sas`
     - `Sqlite`, `PostgreSQL`, `MSSQL`, `MySQL`, `MariaDB`, `Oracle`,
       `SAS`
     - Per-dialect `Sql` subclasses, each with its own query builder
       (e.g. `spinta/datasets/backends/sql/backends/oracle/ufuncs/`).
   * - `dask`, `dask/csv`, `dask/json`, `dask/xml`, `dask/memory`
     - `DaskBackend` and subclasses
     - Data is loaded into a Dask DataFrame and queried with
       `DaskDataFrameQueryBuilder`. `dask/csv` and `dask/memory` do not
       require a source name (`model_requires_source = False`).
   * - `soap`
     - `Soap` (extends `DaskBackend`)
     - Calls a SOAP operation through a `WsdlBackend` client; has its
       own `SoapQueryBuilder`.
   * - `wsdl`
     - `WsdlBackend`
     - Keeps the `zeep` client built from the WSDL URL (the `dsn`).
   * - `sqldump`
     - `SqlDump`
     - Reads SQL dumps from a file or stdin.
   * - `xlsx`, `geojson`, `html`
     - `BackendNotImplemented`
     - Registered placeholders; commands raise `NotImplementedError`.
   * - `csv`, `xml`, `json`
     - `spinta/compat.py` (`CsvDeprecated`, ...)
     - Deprecated pre-dask implementations, kept for bootstrap
       manifests compatibility.

How these backends get attached to models and resources (the `backend`
/ `ref` column, inline `type` + `dsn` backends, keymap encoding) is
described in the datasets material below and in
:doc:`/contrib/architecture/manifests/physical/resources`.

Datasets describe *external* data sources — databases, CSV files, APIs
and similar — from which Spinta pulls data into its internal storage.
While manifests (see :doc:`/contrib/architecture/manifests/index`) describe *what* Spinta serves
(namespaces, models, properties), datasets describe *where the data
comes from* and *how it is mapped* to models.

The dataset classes live in `spinta/datasets/components.py`. They implement
the DCAT idea of a catalog: a catalog contains datasets, a dataset contains
distributions (resources), and each distribution is one accessible form of the
data (https://www.w3.org/TR/vocab-dcat-3/, local extract
`.tmp/sources/w3c/vocab-dcat-3.md`).

Dataset components
******************

`spinta/datasets/components.py` defines these classes:

- `Dataset` — a `MetaData` subclass (see :doc:`/contrib/architecture/concepts/metadata`), one entry
  in the manifest's `MetaDataContainer.dataset` list. Describes metadata
  about an external dataset and holds `resources` (see :doc:`/contrib/architecture/manifests/logical/datasets`).
- `Resource` — an `External` subclass. One accessible form of a dataset's
  data: it points to a backend (`resource.backend`) and a source location
  (`resource.external`, a DSN, URL or file path) (see :doc:`/contrib/architecture/manifests/physical/resources`).
- `Entity` — an `External` subclass. The binding of one *model* to one
  external source: which dataset and resource the model's data comes from,
  which columns are the primary key, and the source table or object name
  (see :doc:`/contrib/architecture/manifests/physical/entities`).
- `Attribute` — an `External` subclass. The binding of one *property* to one
  external column, including an optional type override (see
  :doc:`/contrib/architecture/manifests/physical/attributes`).
- `ExternalBackend` — a `Backend` subclass (from `spinta/backends/
  components.py`) for reading external data, see below.
- `Param` — an `ExtraMetaData` subclass with `source` values and `prepare`
  formulas, used to parametrize source names (e.g. one CSV file per month)
  and HTTP parameters (see `spinta/datasets/utils.py:iterparams()` and
  `spinta/dimensions/param/`).

`External` itself is a trivial `ExtraMetaData` marker: `Resource`, `Entity`
and `Attribute` are all "external" objects attached to internal ones
(dataset, model, property).

Datasets and models
*******************

A model is served by Spinta either from its internal backend or from an
external source. The connection is made in `spinta/types/model.py`:

- When a model row has external columns (`dataset`, `resource`, `pk`,
  `prepare`), model load creates an `Entity` node (`ctype="entity"`) and
  stores it in `model.external`; such a model has
  `mode = Mode.external`.
- `model.external.resource.backend` becomes the model's `backend` if the
  model does not define one itself (see `link(Context, Model)` in
  `spinta/types/model.py`).
- Each property of an external model may get an `Attribute` binding in
  `prop.external`, mapping the property to a source column.

Internal storage is still where data lands when it is pulled (`spinta pull`):
the external side is only used for reading source data. See :doc:`/contrib/architecture/data-access/backends/index`
for the backend classes.

External backends
*****************

`ExternalBackend` extends the generic `Backend` with an SQLAlchemy `engine`
and `schema` (`sa.MetaData`) and a `model_requires_source` flag (most external
backends require a table or object name on the model; `dask/csv` and
`dask/memory` work without one). Concrete types are registered in
`spinta/config.py` under `components.backends` together with the internal
backends (`memory`, `postgresql`, `fs`):

- `sql` (`Sql`, `spinta/datasets/backends/sql/components.py`) — generic SQL
  backend with `BackendFeatures.PAGINATION`, read through SQLAlchemy with the
  `SqlQueryBuilder`. Variants: `sql/sqlite`, `sql/postgresql`, `sql/mssql`,
  `sql/mysql`, `sql/mariadb`, `sql/oracle`, `sql/sas`.
- `dask` (`DaskBackend`) and its formats `dask/csv`, `dask/json`, `dask/xml`,
  `dask/memory` — data is loaded into a Dask DataFrame and queried with the
  `DaskDataFrameQueryBuilder` (`spinta/datasets/backends/dataframe/`).
- `soap` — SOAP services via `zeep`, paired with a `wsdl` backend on the
  resource (see `spinta/datasets/backends/dataframe/backends/soap/`).
- `wsdl` (`WsdlBackend`) — holds the `zeep` client built from the WSDL URL
  given in `resource.external`.
- `sqldump` (`SqlDump`) — reads SQL dump files or streams.
- `xlsx`, `geojson`, `html` — registered placeholders that map to
  `BackendNotImplemented` (`spinta/datasets/backends/notimpl/`), whose
  commands raise `NotImplementedError`.
- `csv`, `xml`, `json` (deprecated, `spinta/compat.py`) — pre-dask
  implementations kept for backwards compatibility.

Note that the type names share the `components.backends` registry with
internal backends: `sql/postgresql` (external, `spinta/datasets/backends/`)
is a different class from `postgresql` (internal, `spinta/backends/`).

Resource backend resolution
***************************

A resource can get its backend in several ways, implemented in
`spinta/datasets/helpers.py:load_resource_backend()`:

1. The `backend` column (`ref` in tabular manifests) names a backend defined
   earlier in the manifest (`manifest.backends`) or in the configuration
   (`store.backends`).
2. Otherwise, if `resource.type` or `resource.external` is given, an inline
   backend is created from the `type` and `dsn` (`external`) columns; the
   dsn can contain `{placeholders}` filled from `resource.params`.
3. Otherwise configuration is probed under the names
   `dataset/resource` and `dataset`, and finally the manifest's default
   backend is used.

Keymap usage in datasets
************************

External rows rarely have globally unique keys, so Spinta maps source keys to
internal UUIDs with a `KeyMap` (see :doc:`/contrib/architecture/keymap/index` for the full
keymap story; `spinta/datasets/keymaps/components.py`):

- `KeyMap` is a `Component` with `encode()` (external to internal),
  `decode()`, `contains()` and sync bookkeeping methods. Implementations:
  `SqlAlchemyKeyMap` (SQLite-backed, `spinta/datasets/keymaps/
  sqlalchemy.py`) and `RedisKeyMap`.
- Keymap instances are configured in the `keymaps` rc section and loaded
  into `store.keymaps` by `load(Context, Store)` (see :doc:`/contrib/architecture/configuration/store`);
  a model refers to one by name (`keymap` column) and it is resolved at
  `link` time — an external model without a keymap raises `KeymapNotSet`.
- During reads, references (`Ref` properties) and composite primary keys are
  encoded through the keymap (`spinta/datasets/commands/cast.py` and
  `spinta/datasets/helpers.py:process_data_for_pkey()`); during `getone`
  the requested `_id` is decoded back to source primary key values.
- The `keymap` synchronization machinery (`spinta/datasets/keymaps/sync.py`)
  yields `KeymapSyncData` rows for pulled data, so `spinta pull` can track
  what has already been imported (`KeyMap.has_synced_before()`,
  `get_last_synced_id()`).

Reading external data
*********************

External data is read with the same `getall`/`getone` commands as internal
data, but dispatched on the *external* backend type:

- `getall(Context, Model, Sql)` (`spinta/datasets/backends/sql/commands/
  read.py`) builds a SQLAlchemy query with the `SqlQueryBuilder` (merging
  the model's `prepare` formula with the user query), executes it and casts
  rows back to model properties.
- `getall(Context, Model, MemoryDaskBackend)` and the generic dask path
  (`spinta/datasets/backends/dataframe/commands/read.py`) load the source
  into a Dask DataFrame, then filter and shape it with
  `DaskDataFrameQueryBuilder`.
- `spinta/datasets/commands/cast.py` converts raw source values to Python
  values (`cast_backend_to_python`) and builds `_id` values, including
  composite and keymap-encoded identifiers.
- `spinta/datasets/utils.py:iterparams()` iterates over resolved `Param`
  values, so one resource can yield several sources (e.g. one file per
  month) and one request can merge them.

Lifecycle
*********

Dataset objects go through the same `load` / `link` / `check` pipeline as
everything loaded from a manifest (see :doc:`/contrib/architecture/multiple-dispatch/commands`):

- `load` — `spinta/datasets/commands/load.py`: `load(Context, Dataset, dict,
  Manifest)` creates the namespace (`load_namespace_from_name`), loads
  prefixes, enums and resources; `load(Context, Resource, ...)` parses the
  `prepare` formula, loads the backend, comments, lang data and params; the
  `Entity` and `Attribute` overloads resolve primary keys and prepare
  formulas. Node creation happens through `spinta/nodes.py:get_node()` with
  `group="datasets"` and `ctype` of `resource`, `entity` or `attribute`.
- `link` — `spinta/datasets/commands/link.py`: resolves string references —
  `Entity.dataset` / `Entity.resource` become real `Dataset` / `Resource`
  objects and the model is registered in `resource.models`; access and level
  inheritance from dataset to resource and model is linked here too.
- `check` — `spinta/datasets/commands/check.py`: checks the dataset name and
  marks datasets/resources as `identifiable` or not (level >=
  `Level.identifiable`).

External specifications
***********************

- DCAT (https://www.w3.org/TR/vocab-dcat-3/, local extract
  `.tmp/sources/w3c/vocab-dcat-3.md`): `Dataset` corresponds to
  `dcat:Dataset`, `Resource` to `dcat:Distribution` / `dcat:DataService`,
  and the Spinta store to a `dcat:Catalog`.
- RML (https://rml.io/specs/rml/, local extract `.tmp/sources/rml/RML.md`):
  the dataset/resource/entity/attribute layer plays the role of RML
  *mapping documents*: `Resource` is the logical source, `Entity` maps a
  triples map's subject (source rows to model instances) and `Attribute`
  maps predicate-object terms (source columns to properties).
- DSA (https://dsa.readthedocs.io/, local extract `.tmp/sources/dsa/DSA.md`)
  defines the tabular `dataset`, `resource` and source columns that load
  into these classes (see :doc:`/contrib/architecture/manifests/tabular-reader`).

See also
********

- :doc:`/contrib/architecture/manifests/logical/datasets`, :doc:`/contrib/architecture/manifests/physical/resources`,
  :doc:`/contrib/architecture/manifests/physical/entities`, :doc:`/contrib/architecture/manifests/physical/attributes` — the dataset classes.
- :doc:`/contrib/architecture/manifests/index` — how manifest rows are parsed into these objects.
- :doc:`/contrib/architecture/manifests/logical/models`, :doc:`/contrib/architecture/manifests/logical/properties` — the internal side the
  external data is mapped to.
- :doc:`/contrib/architecture/multiple-dispatch/ufuncs/index` — the `prepare` formulas used on resources, entities and
  attributes.
