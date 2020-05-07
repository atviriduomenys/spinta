.. default-role:: literal

.. _backend-configuration:

Configuring backends
####################

Spinta supports multiple backends and multiple backends instances. All
supported backends are defined in `components.backends`. You can look it up::

  > spinta config components.backends
  Origin  Name                            Value
  ------  ------------------------------  ------------------------------------------------
  spinta  components.backends.memory      spinta.backends.memory.components:Memory
  spinta  components.backends.postgresql  spinta.backends.postgresql.components:PostgreSQL
  spinta  components.backends.mongo       spinta.backends.mongo.components:Mongo
  spinta  components.backends.fs          spinta.backends.fs.components:FileSystem
  spinta  components.backends.s3          spinta.backends.s3:S3
  spinta  components.backends.sql         spinta.datasets.backends.sql.components:Sql
  spinta  components.backends.csv         spinta.datasets.backends.csv.components:Csv

Each supported backend has a human readable name, like `postgresql` or `mongo`.
You can use these names to add backend instances. Backend instance is a
phisical backend instance for example a specific database running on a server.

If you have multiple multiple PostgreSQL database, you can configure all of
them like this::

  SPINTA_BACKENDS__DB1__TYPE=postgresql
  SPINTA_BACKENDS__DB1__DSN=postgresql://server-1.io/db1

  SPINTA_BACKENDS__DB2__TYPE=postgresql
  SPINTA_BACKENDS__DB2__DSN=postgresql://server-2.io/db2

Now you have configured two `postgresql` instances `db1` and `db2`, each
pointing to a different PostgreSQL database instance.

Try to choose good backend instance names, because these names can be used in
multiple other places, so changing instance name, might be difficult. It is
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

Here, model `reports`, uses a a non-default backend. If at some point you wll
decided to migrate you reports from PostgreSQL to Mongo, all you need to do is
change two configuration options::

  SPINTA_BACKENDS__REPORTS__TYPE=mongo
  SPINTA_BACKENDS__REPORTS__DSN=mongo://server-3.io/db3
