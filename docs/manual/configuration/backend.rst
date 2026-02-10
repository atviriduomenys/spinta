.. default-role:: literal

.. _backend-configuration:

Configuring backends
####################

Operation mode
==============

How backends are configured depends on manifest operation mode.


Spinta supports multiple backends and multiple backend instances. All
supported backends are defined in `components.backends`. You can look it up::

  > spinta config components.backends
    Origin  Name                                Value
    ------  ----------------------------------  ----------------------------------------------------------------------
    spinta  components.backends.memory          spinta.backends.memory.components:Memory
    spinta  components.backends.postgresql      spinta.backends.postgresql.components:PostgreSQL
    spinta  components.backends.mongo           spinta.backends.mongo.components:Mongo
    spinta  components.backends.fs              spinta.backends.fs.components:FileSystem
    spinta  components.backends.sql             spinta.datasets.backends.sql.components:Sql
    spinta  components.backends.sql/sqlite      spinta.datasets.backends.sql.backends.sqlite.components:Sqlite
    spinta  components.backends.sql/postgresql  spinta.datasets.backends.sql.backends.postgresql.components:PostgreSQL
    spinta  components.backends.sql/mssql       spinta.datasets.backends.sql.backends.mssql.components:MSSQL
    spinta  components.backends.sql/mysql       spinta.datasets.backends.sql.backends.mysql.components:MySQL
    spinta  components.backends.sql/mariadb     spinta.datasets.backends.sql.backends.mariadb.components:MariaDB
    spinta  components.backends.sql/oracle      spinta.datasets.backends.sql.backends.oracle.components:Oracle
    spinta  components.backends.sqldump         spinta.datasets.backends.sqldump.components:SqlDump
    spinta  components.backends.dask            spinta.datasets.backends.dataframe.components:DaskBackend
    spinta  components.backends.dask/csv        spinta.datasets.backends.dataframe.backends.csv.components:Csv
    spinta  components.backends.dask/xml        spinta.datasets.backends.dataframe.backends.xml.components.xml:Xml
    spinta  components.backends.dask/json       spinta.datasets.backends.dataframe.backends.json.components:Json
    spinta  components.backends.xlsx            spinta.datasets.backends.notimpl.components:BackendNotImplemented
    spinta  components.backends.geojson         spinta.datasets.backends.notimpl.components:BackendNotImplemented
    spinta  components.backends.html            spinta.datasets.backends.notimpl.components:BackendNotImplemented
    spinta  components.backends.csv             spinta.compat:CsvDeprecated
    spinta  components.backends.xml             spinta.compat:XmlDeprecated
    spinta  components.backends.json            spinta.compat:JsonDeprecated

Certain backends allow specifying a sub-backend for tailored functionality.
Currently there are two backends that allow it:

- `sql` - Generic `sql` backend that support most of the `sql` dialects. It is highly recommended to specify the dialect.

  - `sql/sqlite` - Sqlite dialect.
  - `sql/postgresql` - PostgreSQL dialect.
  - `sql/mssql` - Microsoft SQL dialect.
  - `sql/mysql` - MySQL dialect.
  - `sql/mariadb` - MariaDB dialect.
  - `sql/oracle` - Oracle dialect.

- `dask` - `Backend` type that focuses on storing data into dask dataframe tables. It must specify sub-backend.

  - `dask/csv` - Csv format (formally `csv`).
  - `dask/xml` - Xml format (formally `xml`).
  - `dask/json` - Json format (formally `json`).

Each supported backend has a human readable name, like `postgresql` or `mongo`.
You can use these names to add backend instances. Backend instance is a
physical backend instance, for example a specific database running on a server.

If you have multiple multiple PostgreSQL database, you can configure all of
them like this::

  SPINTA_BACKENDS__DB1__TYPE=postgresql
  SPINTA_BACKENDS__DB1__DSN=postgresql://server-1.io/db1

  SPINTA_BACKENDS__DB2__TYPE=postgresql
  SPINTA_BACKENDS__DB2__DSN=postgresql://server-2.io/db2

Now you have configured two `postgresql` instances `db1` and `db2`, each
pointing to a different PostgreSQL database instance.

Try to choose good backend instance names, because these names can be used in
multiple other places, so changing instance name might be difficult. It is
better to choose names, that align with you data model.

For example, these names might be better choice::

  SPINTA_BACKENDS__DEFAULT__TYPE=postgresql
  SPINTA_BACKENDS__DEFAULT__DSN=postgresql://server-1.io/db1

  SPINTA_BACKENDS__REPORTS__TYPE=postgresql
  SPINTA_BACKENDS__REPORTS__DSN=postgresql://server-2.io/db2

Next thing when configuring backends is to set default backend for you default
manifest (see :ref:`manifest-configuration`)::

  SPINTA_MANIFEST=default
  SPINTA_MANIFESTS__DEFAULT__BACKEND=default

All manifest models, will use default manifest backend if backend is not
explicitly set on a model.

Each model, can explicitly set backend like this:

.. code-block:: yaml

  ---
  type: model
  name: reports
  backend: reports

Here, model `reports`, uses a a non-default backend. If at some point you will
decide to migrate you reports from PostgreSQL to Mongo, all you need to do is
change two configuration options::

  SPINTA_BACKENDS__REPORTS__TYPE=mongo
  SPINTA_BACKENDS__REPORTS__DSN=mongo://server-3.io/db3
