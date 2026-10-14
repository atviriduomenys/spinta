.. default-role:: literal

########
Backends
########

In C4 terms (see :doc:`/contrib/architecture/index`), the internal backend is one of
Spinta's :ref:`containers <contrib-containers>` (the data store side;
external source backends belong to the :ref:`external data stores
<contrib-containers>` container, see :doc:`/contrib/architecture/data-access/backends/external-data-sources`).

A backend is where a model's data lives. Spinta has two families of
backends, both registered in the same `config.components['backends']`
registry (`spinta/config.py:CONFIG`):

- **Internal backends** (`spinta/backends/`) — where Spinta *stores* data
  it manages itself: manifest data pulled from external sources, write
  requests coming through the API, schemas and changelogs.
  Types: `postgresql`, `fs` (file system), `memory` (tests) and the
  `NoBackend` placeholder.
- **External source backends** (`spinta/datasets/backends/`) — *read-only*
  drivers for external data sources: `sql` (with per-dialect subclasses),
  `dask` (CSV/JSON/XML readers), `soap`/`wsdl`, `sqldump`.

The internal/external split follows the model modes: a model with
`mode internal` stores its data in the manifest's internal backend, while
`mode external` models read data straight from the source through an
external backend (see :doc:`/contrib/architecture/wired-together` and :doc:`/contrib/architecture/data-access/backends/external-data-sources`).

The base `Backend` class itself is described in a dedicated concept page
(see :doc:`backend`), external source backends in :doc:`external-backend`.

How backends are registered and instantiated
********************************************

Classes. The `components.backends` section of `spinta/config.py:CONFIG`
maps backend type names to classes, e.g.::

   "backends": {
       "memory": "spinta.backends.memory.components:Memory",
       "postgresql": "spinta.backends.postgresql.components:PostgreSQL",
       "fs": "spinta.backends.fs.components:FileSystem",
       "sql": "spinta.datasets.backends.sql.components:Sql",
       "sql/sqlite": "spinta.datasets.backends.sql.backends.sqlite.components:Sqlite",
       ...
   }

Note that one registry holds both families: `sql/postgresql` (`Sql`
subclass in `spinta/datasets/backends/`) is a different class from
`postgresql` (`spinta/backends/postgresql/components.py:PostgreSQL`).

Configuration. Runtime instances are created from the `backends`
configuration section (`rc.keys('backends')`), an object with arbitrary
keys — backend names — and values like::

   backends:
     default:
       type: postgresql
       dsn: postgresql://admin:admin123@localhost:54321/spinta

The `backends` schema in `spinta/config.yml` is a dynamic-key group whose
`switch: type` cases add type specific options (e.g. the `s3` case
currently exists in the schema as a placeholder; the `sql` cases take a
`dsn`).

Instantiation. `load(Context, Store)` (`spinta/types/store.py`) walks the
`backends` keys and calls `load_backend()` (`spinta/backends/helpers.py`)
for each entry. `load_backend()` looks up the class in
`config.components['backends']`, sets `type`, `name`, `origin`,
`config`, resolves query/result builder classes and finally dispatches
`commands.load(Context, Backend, dict)` — a per-backend overload that
connects the backend (e.g. the `PostgreSQL` overload in
`spinta/backends/postgresql/commands/load.py` builds the SQLAlchemy
engine and `sa.MetaData`). Failed loads raise `BackendNotFound`.

Origins. `Backend.origin` (`BackendOrigin` in
`spinta/backends/constants.py`) records where the backend was defined:

- `config` — from the `backends` section of the configuration;
- `manifest` — declared in a manifest, collected into
  `Manifest.backends` (`spinta/manifests/helpers.py`, see :doc:`/contrib/architecture/manifests/index`);
- `resource` — declared inline on a `Resource` (from the `type` and
  `dsn`/`external` columns), created by `load_resource_backend()` in
  `spinta/datasets/helpers.py` (see :doc:`/contrib/architecture/data-access/backends/external-data-sources`).

Backends and models. Each `Model` gets its `backend` during
`commands.link` (`link(Context, Model)` in `spinta/types/model.py`): a
model-defined backend name resolves through `model.manifest.backends`
(backend manifests) or `manifest.store.backends` (configuration), an
external model takes `model.external.resource.backend`, external models
without a source get `NoBackend()`, and everything else falls back to the
manifest backend (`store.manifest.backend`).

External specifications
***********************

- UAPI (https://github.com/atviriduomenys/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`): the spec is deliberately storage
  agnostic — it describes the JSON resources and actions (`getall`,
  `getone`, `push`, ...), not the backend. Spinta implements it by
  converting backend data (PostgreSQL rows, external source records) to
  UAPI responses.
- DCAT (https://www.w3.org/TR/vocab-dcat-3/, local extract
  `.tmp/sources/w3c/vocab-dcat-3.md`): internal backends implement data
  *storage* (datasets and their distributions in Spinta terms), while an
  external source backend plays the role of a `dcat:DataService` — a
  service giving access to a `dcat:Distribution` of a dataset.
- RML (https://rml.io/specs/rml/, local extract `.tmp/sources/rml/
  RML.md`): RML is about mapping *source* data into RDF; the
  dataset/external-backend layer is the Spinta counterpart of RML's
  logical sources, while internal backends are the target where the
  copied data lands.

See also
********

- :doc:`/contrib/architecture/data-access/backends/backend` — the `Backend` base class.
- :doc:`/contrib/architecture/data-access/backends/external-backend` — the `ExternalBackend` base class
  and registered external types.
- :doc:`/contrib/architecture/data-access/backends/external-data-sources` — external backends and the datasets layer.
- :doc:`/contrib/architecture/data-access/pagination` — `BackendFeatures.PAGINATION` and pagination.
- :doc:`/contrib/architecture/multiple-dispatch/ufuncs/index` — query/result builder ufunc environments.
- :doc:`/reference/components` — autodoc of component classes.
- `spinta/backends/constants.py` — `BackendFeatures`, `TableType`,
  `BackendOrigin`, `DistributionType`.

Pages
*****

- :doc:`internal-data-store` — internal backends where Spinta stores
  data it manages (PostgreSQL incl. Citus/PostGIS, memory, fs,
  nobackend), transactions and query/result builders.
- :doc:`external-data-sources` — external, read-only source backends
  (sql and its dialects, dask csv/json/xml, soap/wsdl, sqldump) and
  how they read data.

.. toctree::
   :maxdepth: 1

   Internal data store <internal-data-store>
   External data sources <external-data-sources>


