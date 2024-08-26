.. default-role:: literal

Spinta is a command line tool and REST JSON API service for publishing and
mapping data between different physical data models, JSON API and semantic data
models. It supports a great deal of data schemes and formats.

.. image:: https://gitlab.com/atviriduomenys/spinta/badges/master/pipeline.svg
   :target: https://gitlab.com/atviriduomenys/spinta/commits/master

.. image:: https://gitlab.com/atviriduomenys/spinta/badges/master/coverage.svg
   :target: https://gitlab.com/atviriduomenys/spinta/commits/master

::

    Physical data        Different       Real time         REST JSON
       sources            formats      transformation         API
                    
                          +-----+
                     +--> | SQL | --------->|
       +------+      |    +-----+           |
       | file | ---->|                      |
       +------+      |    +-----+           |
                     +--> | CSV | --------->|
      +--------+     |    +-----+           |         +---------------+
      | DB API | --->|                      | ------->| REST JSON API |
      +--------+     |    +------+          |         +---------------+
                     |    | JSON | -------->|
     +----------+    |    +------+          |
     | REST API | -->|                      |
     +----------+    |    +-----+           |
                     +--> | XML | --------->|
                          +-----+




Purpose
=======

- **Describe your data**: You can automatically generate data structure
  description table (*Manifest*) from many different data sources.

- **Extract your data**: Once you have your data structure in *Manifest* tables,
  you can extract data from multiple external data sources. Extracted data are
  validated and transformed using rules defined in *Manifest* table. Finally,
  data can be stored into internal database in order to provide fast and
  flexible access to data.
  
- **Transform your data**: Data transformations are applied in real time, when
  reading data from source. This puts some limitations on transformation side, but
  allows data to be streamed in real time.

- **Publish your data**: Once you have your data loaded into internal
  database, you can publish data using API. API is generated automatically using
  *Manifest* tables and provides extracted data in many different formats. For
  example if original data source was a CSV file, now you have a flexible API,
  that can talk JSON, RDF, SQL, CSV and other formats.


Features
========

- Simple 15 column table format for describing data structures (you can use
  any spreadsheet software to manage metadata of your data)

- Internal data storage with pluggable backends (PostgreSQL or Mongo)

- Build-in async API server built on top of Starlette_ for data publishing

- Simple web based data browser.

- Convenient command-line interface

- Public or restricted API access via OAuth protocol using build-in access
  management.

- Simple DSL_ for querying, transforming and validating data.

- Low memory consumption for data of any size

- Support for many different data sources

- Advanced data extraction even from dynamic API.

- Compatible with DCAT_ and `Frictionless Data Specifications`_.

.. _Starlette: https://www.starlette.io/
.. _DSL: https://en.wikipedia.org/wiki/Domain-specific_language
.. _DCAT: https://www.w3.org/TR/vocab-dcat-2/
.. _Frictionless Data Specifications: https://specs.frictionlessdata.io/


.. note::

    Currently this project is under heavy development and is in pre-alpha stage.
    So many things might not work and many things can change. Use it at your own
    risk.


Example
=======

If you have an SQLite database:

.. code-block:: sh

    $ sqlite3 sqlite.db <<EOF
    CREATE TABLE COUNTRY (
        NAME TEXT
    );
    EOF

You can get a limited API and simple web based data browser with a single
command:

.. code-block:: sh

    $ spinta run -r sql sqlite:///sqlite.db

Then you can generate metadata table (*manifest*) like this:

.. code-block:: sh

    $ spinta inspect -r sql sqlite:///sqlite.db
    d | r | b | m | property | type   | ref | source              | prepare | level | access | uri | title | description
    dataset                  |        |     |                     |         |       |        |     |       |
      | sql                  | sql    |     | sqlite:///sqlite.db |         |       |        |     |       |
                             |        |     |                     |         |       |        |     |       |
      |   |   | Country      |        |     | COUNTRY             |         |       |        |     |       |
      |   |   |   | name     | string |     | NAME                |         | 3     | open   |     |       |

Generated data structure table can be saved into a CSV file:

.. code-block:: sh

    $ spinta inspect -r sql sqlite:///sqlite.db -o manifest.csv

Missing peaces in metadata can be filled using any Spreadsheet software.

Once you done editing metadata, you can test it via web based data browser or
API:

.. code-block:: sh

    $ spinta run --mode external manifest.csv

Once you are satisfied with metadata, you can generate a new metadata table for
publishing, removing all traces of original data source:

.. code-block:: sh

    $ spinta copy --no-source --access open manifest.csv manifest-public.csv

Now you have matadata for publishing, but all things about original data
source are gone. In order to publish data, you need to copy external data to
internal data store. To do that, first you need to initialize internal data
store:

.. code-block:: sh

    $ spinta config add backend my_backend postgresql postgresql://localhost/db
    $ spinta config add manifest my_manifest tabular manifest-public.csv
    $ spinta migrate

Once internal database is initialized, you can push external data into it:

.. code-block:: sh

    $ spinta push --access open manifest.csv

And now you can publish data via full featured API with a web based data
browser:

.. code-block:: json

    $ spinta run

You can access your data like this:

.. code-block:: json

    $ http :8000/dataset/sql/Country
    HTTP/1.1 200 OK
    content-type: application/json

    {
        "_data": [
            {
                "_type": "dataset/sql/Country",
                "_id": "abdd1245-bbf9-4085-9366-f11c0f737c1d",
                "_rev": "16dabe62-61e9-4549-a6bd-07cecfbc3508",
                "_txn": "792a5029-63c9-4c07-995c-cbc063aaac2c",
                "name": "Vilnius"
            }
        ]
    }

    $ http :8000/dataset/sql/Country/abdd1245-bbf9-4085-9366-f11c0f737c1d
    HTTP/1.1 200 OK
    content-type: application/json

    {
        "_type": "dataset/sql/Country",
        "_id": "abdd1245-bbf9-4085-9366-f11c0f737c1d",
        "_rev": "16dabe62-61e9-4549-a6bd-07cecfbc3508",
        "_txn": "792a5029-63c9-4c07-995c-cbc063aaac2c",
        "name": "Vilnius"
    }

    $ http :8000/dataset/sql/Country/abdd1245-bbf9-4085-9366-f11c0f737c1d?select(name)
    HTTP/1.1 200 OK
    content-type: application/json

    {
        "name": "Vilnius"
    }
