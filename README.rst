This data store allows you to take full control of your data.

.. image:: https://travis-ci.com/atviriduomenys/spinta.svg?branch=master
   :target: https://travis-ci.com/atviriduomenys/spinta

.. image:: https://codecov.io/gh/atviriduomenys/spinta/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/atviriduomenys/spinta


Features
========

- Support for multiple backends, currently supported backends: PostgreSQL.

- Declarative data descriptors using YAML format.

- Change history, you can restore and view historical data.

- Import data from external data sources.


Development environment
=======================

Create database::

   createdb spinta

Create configuration files::

   SPINTA_BACKENDS_DEFAULT_DSN=postgresql:///spinta
   SPINTA_MANIFESTS_DEFAULT_PATH=path/to/manifest

Prepare environment::

   make

Run database migrations::

   env/bin/spinta migrate


Import some datasets::

   env/bin/spinta pull gov/vrk/rinkejopuslapis.lt/kandidatai
   env/bin/spinta pull gov/lrs/ad
