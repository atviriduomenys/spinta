.. default-role:: literal

This data store allows you to take full control of your data.

.. image:: https://gitlab.com/atviriduomenys/spinta/badges/master/pipeline.svg
   :target: https://gitlab.com/atviriduomenys/spinta/commits/master

.. image:: https://gitlab.com/atviriduomenys/spinta/badges/master/coverage.svg
   :target: https://gitlab.com/atviriduomenys/spinta/commits/master


Features
========

- Support for multiple backends, currently supported backends: PostgreSQL.

- Declarative data descriptors using YAML format.

- Change history, you can restore and view historical data.

- Import data from external data sources.


Configuration
=============

Spinta reads configuration values from these sources, listed in priority order:

- Command line arguments passed as `-o key=value`.

- From environment variables, first looking into `SPINTA_<env>_<name>`, then to
  `SPINTA_<name>`. `<env>` is taken from `SPINTA_ENV`.

- From environment file `.env`, using same naming as environment variables.

- From default configuration, specified by `SPINTA_CONFIG`, which defaults to
  `spinta.config:CONFIG`.

When running tests, Spinta uses `test` as environment name, so if you need to
set configuration parameters only for tests, use `SPINTA_TEST_<name>`. For
example, if you have these environment variables set::

   SPINTA_BACKENDS_DEFAULT_DSN=a
   SPINTA_TEST_BACKENDS_DEFAULT_DSN=b

Then, when you run tests, `b` will be used, in all other cases, `a` will be
used.
