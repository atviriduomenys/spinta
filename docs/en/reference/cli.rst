.. default-role:: literal

##########
CLI
##########

.. _cli_inspect:

inspect
=======

The ``inspect`` CLI command reads the schema of an external data source, such as
a database table, and writes it as a manifest. Models, properties and their
external sources are taken from the data source, and the manifest given as an
argument is updated with them.

Usage
-----

.. code-block:: bash

   spinta inspect [<manifest>] [-r <type> <source>] [-b <backend>] [-o <path-to-output-file>]

Arguments
---------

- ``<manifest>``
  OPTIONAL. Path to the manifest to update. Without it, a new manifest is
  created from the data source.

Options
-------

- ``-r``, ``--resource <type> <source>``
  Resource type and source URI, for example ``-r sql sqlite:////tmp/db.sqlite``.

- ``-f``, ``--formula <formula>``
  Formula, if needed, to prepare the resource for reading.

- ``-b``, ``--backend <connection-string>``
  Backend connection string.

- ``-o``, ``--output <path-to-output-file>``
  OPTIONAL. Write the manifest to this file, for example ``manifest.csv`` or
  ``manifest.xlsx``. Without it, the manifest is printed as a table.

- ``-a``, ``--auth <client>``
  Authorize as a client.

- ``-p``, ``--priority <manifest|external>``
  Which value wins for a property that both the manifest and the data source
  define, such as its ``type``, ``uri`` or ``description``. ``manifest``
  (default) keeps the manifest value, ``external`` takes the value from the data
  source. A value that only one of them has is always used.

- ``--count``
  OPTIONAL. Count rows and values in the data source and fill the ``count``
  column, see `inspect --count`_.

Example
-------

**Command**:

.. code-block:: bash

   spinta inspect sqlite:////data.db -o manifest.csv

The manifest is written to ``manifest.csv``, with one model for each table of
the database. Use ``--count`` to fill the ``count`` column as well.

.. _cli_inspect_count:

inspect --count
---------------

With ``--count``, the ``count`` column of the manifest is filled from the data
source. Each dataset, model, property and enum value gets a count:

- dataset: the number of models of the dataset,
- model: the number of rows in the model's table,
- property: the number of non-NULL values in the property's column,
- enum value: the number of rows whose column has the enum value, that is the
  ``source`` of the enum item. Counts of the same enum are summed over all the
  properties that use it, and the sum is empty if any of them can not be counted.

A count is left empty when it can not be made. This happens for a model or
property without a table or column in the data source, and for a data source
that does not support counting, such as a JSON resource. A database error
is logged as a warning, and the count is left empty, without stopping the
command. The dataset count is always filled.

Each count is a separate query on the table, so counting a large table takes
longer than inspecting its schema.

**Command**:

.. code-block:: bash

   spinta inspect --count sqlite:////data.db -o manifest.csv

**Output** for a SQLite database with four tables, shortened. ``Apskritis``
has ``10`` rows, and ``Gyvenviete`` has ``26231`` rows, all of them with a value
in ``gyv_kodas``:

.. code-block:: text

   d | r | m | property    | type    | source      | count
   data_db               |         |             | 4
     | resource1         | sql     |             |
     |   | Apskritis     |         | Apskritis   | 10
     |   |   | adm_kodas   | integer | adm_kodas   | 10
     |   |   | pavadinimas | string  | pavadinimas | 10
     |   | Gyvenviete    |         | Gyvenviete  | 26231
     |   |   | gyv_kodas   | integer | gyv_kodas   | 26231

The count of the dataset, ``4``, is the number of its models.

.. _cli_getall:

getall
======

The ``getall`` CLI command reads a YAML file and returns its JSON representation.

Usage
-----

.. code-block:: bash

   spinta getall <manifest-files-to-load> <path-to-yaml-file> <dataset-name>

Arguments
---------

- ``<manifest-files-to-load>``
  One or more manifest files that will be loaded.

- ``<path-to-yaml-file>``
  Path to the YAML file to be read. This file should be properly structured and conform to the manifest schema.

- ``<dataset-name>``
  The name of the dataset, as defined in the YAML file. This must match the dataset name specified in the manifest.

Example
-------

**Input YAML File (`config.yaml`)**:

.. code-block:: yaml

    _type: datasets/gov/example/City
    _id: 0AF24A60-00A2-4EAB-AEFF-BBA86204BC98
    name: Vilnius
    country:
      _id: 4689C28B-1C44-4184-8715-16021EE87EAD
      name: Lietuva
    ---
    _type: datasets/gov/example/Country
    _id: 4689C28B-1C44-4184-8715-16021EE87EAD
    name: Lietuva

**Command**:

.. code-block:: bash

   spinta getall manifest.csv config.yaml datasets/gov/example/City

**Output**:

.. code-block:: json

   {
  "_data": [
    {
      "_type": "datasets/gov/example/City",
      "_id": "0AF24A60-00A2-4EAB-AEFF-BBA86204BC98",
      "name": "Vilnius",
      "country": {
        "_id": "4689C28B-1C44-4184-8715-16021EE87EAD"
      }
    }
  ]
}

.. _cli_copy:

copy
====

The ``copy`` CLI command reads a manifest file and depending on arguments either returns manifest in tabular format or writes into file.

Usage
-----

.. code-block:: bash

   spinta copy <manifest-files-to-load> -o <path-to-output-file> -d <dataset-name>

Arguments
---------

- ``<manifest-files-to-load>``
  One or more manifest files that will be loaded.

- ``<path-to-output-file>``
  OPTIONAL. Output file name. If specified, manifest will be written to this file. If file extension is `.mmd`, UML diagram will be generated in Mermaid syntax. If file extension is `.ttl`, manifest will be written as RDF Turtle, see `RDF Turtle output`_. If file extension is `.jsonld`, JSON-LD context documents are written, see `JSON-LD context output`_.

- ``<dataset-name>``
  The name of the dataset. For Mermaid diagram, the specified dataset will not be wrapped in Mermaid class diagram `namespace <https://mermaid.js.org/syntax/classDiagram.html#define-namespace>`_. For RDF Turtle and JSON-LD, only models of the specified dataset are written, see `Dataset selection`_. The dataset must exist in the manifest, otherwise the command fails.

- ``-m <model-name>``
  JSON-LD only: write only this model, as its context document, see `JSON-LD context output`_.

Default bases
-------------

The ``-b``, ``--base`` option gives a default base manifest. A model that has no base is matched to the one model of the same name, without its dataset, in the default bases, and the matched model becomes its base. The base is written as a ``base`` row, with the keys of the matched model as ``ref``:

.. code-block:: bash

   spinta copy ar.txt -b locn.txt -o manifest.csv

If ``locn.txt`` has ``Country`` in the ``locn`` dataset, then ``datasets/gov/rc/ar/uapi/addr/Country`` in ``ar.txt`` gets the base ``/locn/Country``. The option can be given more than once.

- A model with no match, or with more than one model of its name in the default bases, gets a ``DefaultBaseNotFound`` error. The error is printed on stderr and counted in ``Total errors``. The output is still written, and the exit status stays ``0``.
- ``--base-visibility`` takes a comma separated list of ``public``, ``package``, ``protected`` and ``private``. Only models with one of these visibilities get the error. By default all models do.
- The default base manifests are read only to find the matches. Their models are not written, but the values of their models are inherited (see `Inherited values`_).
- The option works with ``csv``, ``ascii``, ``xlsx`` and ``simple`` inputs, and with table, CSV, XLSX, Mermaid, RDF Turtle and ``simple`` outputs.
- A model matched to a default base has no ``rdfs:subClassOf`` in RDF Turtle output, because the base model is not loaded.

Included manifests
------------------

The ``-i``, ``--include`` option gives a manifest whose elements the references of the source manifests need. Only the elements that the references need are copied from it, and the rest of the manifest is ignored:

.. code-block:: bash

   spinta copy dsa.txt -i include.txt -o manifest.csv

If ``dsa.txt`` has ``country`` with the ``ref`` ``/locn/Country``, and ``include.txt`` defines ``Country`` in the ``locn`` dataset, then ``Country`` is copied with its dataset and its key properties. Its other properties are not copied. The option can be given more than once.

- A referenced element is copied with the elements it needs: its dataset, its base, the properties that are its keys, the prefixes of its ``uri``, and the items of the enums it uses. Those elements are copied in the same way, so a chain of references is followed.
- The included manifests are read in the same formats as the source manifests: ``csv``, ``ascii``, ``xlsx`` and ``simple``.
- In a ``.simple.json`` output, an included element has no ``index``, and keeps the ``given`` it has in its included manifest, with its ``implied``. Its ``given`` is what the included manifest wrote, so the output reads back with its included elements, and copying it to CSV gives the same rows as copying the source with ``-i``.
- A reference that is not in the source manifests or in an included manifest stays unresolved, as it is without ``-i``.

Inherited values
----------------

A model or a property that does not set ``status``, ``visibility``, ``access``, ``uri``, ``eli``, ``title`` or ``description`` takes the value from its nearest base that sets it. This is done for ``spinta copy``, and for ``Spinta.copy()`` in the library, whatever the input and output formats are:

- a model takes it from its base model, a property from the property of its base model with the same name;
- datasets and resources are not inherited;
- the access of a base is taken before the access of the owner (the dataset of a model, or the model of a property). The owner's access is used only when no base sets one.

A value is taken only when the element does not set it. The ``uri`` of a base is written with a prefix of the dataset of the element: the prefix that has the same IRI, if the dataset has one under another name, else the same prefix name when it is free there (it is then added to the dataset), else the full IRI. A ``uri`` with an undeclared prefix is not taken.

For example, with ``locn.txt`` that has ``Country`` with ``uri`` ``locn:Country`` and ``title`` ``Country`` in the ``locn`` dataset, the ``Country`` model of ``ar.txt`` gets the same ``uri`` and ``title`` when it has none, and the ``locn`` prefix is added to its dataset:

.. code-block:: bash

   spinta copy ar.txt locn.txt -o manifest.csv

A ``simple`` output keeps ``given`` as it was written, and fills in the inherited values in ``implied``. Other outputs write the inherited values as if they were given.

Implied values
--------------

The ``--implied`` option writes the ``implied`` value of a cell that has no given value, in a table output (``csv``, ``xlsx`` and ``ascii``). A ``.simple.json`` file keeps ``implied``, so a table written from it shows the values that the file has, such as a title that a base gave:

.. code-block:: bash

   spinta copy own.simple.json -o manifest.csv --implied

- It fills the ``title``, ``description``, ``eli``, ``status``, ``visibility`` and ``access`` of datasets, models and properties. A given value is never replaced. ``uri`` is not filled: ``implied`` has a URI for every element, the one Spinta derives when none is given.
- Without the option, a table has the given values only, so it can be read back with them as given. A table written with ``--implied`` shows the effective values, for reading.
- A CSV, XLSX or ASCII input has no ``implied`` stored with it, so it gets the ``implied`` that ``copy`` to ``simple`` gives it. The table is then the same as the one from its ``simple`` document.
- The option does not take ``--base``. The ``implied`` of a document is the one it was written with, and a base of the copy would change it. Write the document with its base first, then copy it to the table.
- A ``simple``, ``mermaid`` or ``rdf`` output does not take the option.

Mermaid.js output
-----------------

Example
~~~~~~~

**Input Manifest file (`manifest.csv`)**:

+----+-----------------+----------+------+---------+----------+---------+---------+
| id | dataset         | resource | base | model   | property | type    | ref     |
+====+=================+==========+======+=========+==========+=========+=========+
|    | example/dataset |          |      |         |          |         |         |
+----+-----------------+----------+------+---------+----------+---------+---------+
|    |                 | resource |      |         |          |         |         |
+----+-----------------+----------+------+---------+----------+---------+---------+
|    |                 |          |      | City    |          |         |         |
+----+-----------------+----------+------+---------+----------+---------+---------+
|    |                 |          |      |         | id       | integer |         |
+----+-----------------+----------+------+---------+----------+---------+---------+
|    |                 |          |      |         | name     | string  |         |
+----+-----------------+----------+------+---------+----------+---------+---------+
|    |                 |          |      |         | city     | ref     | Country |
+----+-----------------+----------+------+---------+----------+---------+---------+
|    |                 |          |      | Country |          |         |         |
+----+-----------------+----------+------+---------+----------+---------+---------+
|    |                 |          |      |         | id       | integer |         |
+----+-----------------+----------+------+---------+----------+---------+---------+
|    |                 |          |      |         | name     | string  |         |
+----+-----------------+----------+------+---------+----------+---------+---------+


**Command**:

.. code-block:: bash

   spinta copy manifest.csv -o manifest.mmd -d example/dataset

**Output (manifest.mmd)**:

.. code-block:: mermaid

  ---
  config:
    theme: base
    themeVariables:
      mainBkg: '#ffffff00'
      clusterBkg: '#ffffde'
  ---

  classDiagram
  class `example/dataset/City`["City"]:::Entity {
  «optional»
  id : integer [0..1]
  name : string [0..1]
  }
  class `example/dataset/Country`["Country"]:::Entity {
  «optional»
  id : integer [0..1]
  name : string [0..1]
  }

  `example/dataset/City` --> "[0..1]" `example/dataset/Country` : city<br/>«optional»
  classDef Concept stroke:#8FB58F,fill:#F0FDF0,color:#000000;
  classDef Entity stroke:#9D8787,fill:#F5E8DF,color:#000000;

Enums with many items
~~~~~~~~~~~~~~~~~~~~~

An enum is drawn with at most ``uml_max_enum_items`` items, which is ``50``
by default, so that very large enums do not make the diagram unreadable. The
remaining items are replaced by one ``(N more...)`` item, where ``N`` is the
number of items that are not drawn:

.. code-block:: bash

   spinta -o uml_max_enum_items=20 copy manifest.csv -o manifest.mmd

Mermaid does not render a ``(N more...)`` line directly, so it is written with
entities, ``#40;N more...#41;``, which Mermaid displays as ``(N more...)``.

JSON-LD context output
----------------------

A manifest written to a ``.jsonld`` file is a set of JSON-LD context documents,
one per model, which are the ``@context`` of the model's objects in the JSON-LD
responses. The IRIs of the terms are the ones the ``simple`` output gives, see
``implied.uri``, and they are built from the ``uri`` configuration option
(``SPINTA_URI``), so set it first. Without ``uri`` the IRIs are relative, and
the command logs a warning.

Without ``-m`` the file is one JSON object: each key is a model name, and each
value is that model's context document. A ``protected`` or ``private`` model has
no context, so it is not in the file.

.. code-block:: bash

   spinta -o uri=https://data.gov.lt copy manifest.csv -o contexts.jsonld
   spinta -o uri=https://data.gov.lt copy manifest.csv -m datasets/gov/vssa/demo/Country -o Country.jsonld

With ``-m``, the file is the context document of that model itself. It is hosted
as ``{uri}/context/{model path}.jsonld``, and Spinta does not serve it. ``-m``
does not take a table or ``simple`` output, and it fails for an unknown model, a
``protected`` or ``private`` model, or a model outside the ``-d`` dataset.

RDF Turtle output
-----------------

A manifest written to a ``.ttl`` file is described with SHACL shapes. Every
model and property is described by a shape, which validates the values of the
model. Classes, properties and concepts are also described with RDFS classes,
RDFS properties and SKOS concepts, if they are described in the file, see
`Which items are described`_. Enums are described as SKOS concept schemes.

Which items are described
~~~~~~~~~~~~~~~~~~~~~~~~~

A class, property or concept is described in the file if:

- it has no ``uri``, or
- its ``uri`` is under ``uri_prefix``, either as a full IRI, such as
  ``https://data.gov.lt/id/datasets/gov/example/Country``, or as a prefixed
  name, such as ``ex:Country`` when ``ex`` is declared as
  ``https://data.gov.lt/id/``, or
- its ``uri`` is a relative path. A relative path is relative to ``uri_prefix``,
  so ``uri: Country`` is ``{uri_prefix}Country``.

Other items, for example ``uri: dct:title`` or ``uri: https://schema.org/Person``,
are only referenced by their ``uri``. Their definitions are not written to the
file. Enum concept schemes are always generated.

Generated URIs
~~~~~~~~~~~~~~

Generated URIs are built from the ``uri_prefix`` configuration option, for
example ``-o uri_prefix=https://data.gov.lt/id/`` or
``SPINTA_URI_PREFIX=https://data.gov.lt/id/``. If it is not set, the default
``https://example.com`` is used. A trailing slash in ``uri_prefix`` is
optional:

.. code-block:: bash

   spinta -o uri_prefix=https://data.gov.lt/id/ copy manifest.csv -o manifest.ttl

Generated URIs follow the persistent URI rules of the `Interoperable Europe
SEMIC support centre
<https://interoperable-europe.ec.europa.eu/collection/semic-support-centre/document/10-rules-persistent-uris>`_,
``{uri_prefix}{type}/{dataset}/{concept}/{reference}``. The type is ``id`` for
models and properties, and ``def`` for concept schemes and concepts. Generated
URIs use these patterns:

- classes: ``{uri_prefix}id/{dataset}/{model}``
- properties: ``{uri_prefix}id/{dataset}/{model}/{property}``. A property has
  its own URI in each model. A property with a ``uri`` keeps it, and is shared
  by the models that give it.
- concept schemes (enums): ``{uri_prefix}def/{dataset}/{enum}``. Inline enums
  are named after their model and property, for example
  ``DatasetEvent_type``.
- concepts (enum items): ``{uri_prefix}def/{dataset}/{enum}/{code}``, where
  ``{code}`` is percent-encoded, so ``CITY/TOWN`` is ``CITY%2FTOWN``. Concepts
  of different enums with the same code get different URIs.
- shapes: ``{uri_prefix}shape/{dataset}/{model}``. Each model has one SHACL
  node shape with this IRI, so models that share a class get separate shapes.

A model without a ``uri`` inherits the class IRI of its ``base``, so the
``base`` chain shares one class. Only the model that owns the class describes
it, with ``rdfs:Class``, its label and its comment. The base link is written as
``sh:node`` between the shapes. ``rdfs:subClassOf`` is written only when the
class IRI of the model differs from the class IRI of its base and the model's
class is described in the file. For example, a model with its own ``uri`` under
``uri_prefix`` whose base has ``uri: dct:Location`` gets ``rdfs:subClassOf``
``dct:Location``.

If there is no dataset, the ``{dataset}/`` part is left out.

Dataset selection
~~~~~~~~~~~~~~~~~

By default all datasets of the manifest are written. With ``-d <dataset-name>``
only the models of that dataset are written, together with their classes,
shapes, properties and enums. Items of other datasets are not written, but
references to them keep their IRIs, for example ``sh:class`` of a property that
refers to a model of another dataset. An enum declared by a parent namespace
keeps the IRI of its own dataset, and is written only by that dataset.

.. code-block:: bash

   spinta -o uri_prefix=https://data.gov.lt/id/ copy manifest.csv -d example/dataset -o manifest.ttl

A base model of another dataset is not referenced by ``sh:node``, so the shape
does not inherit its constraints. This is the same as for a base that is not
exported because of ``access``.

If a ``uri`` uses a prefix that is not declared in the manifest, the command
fails with an error, instead of writing a wrong URI.

The file is split into sections, each introduced by a comment: prefixes,
classes, properties, concepts and SHACL shapes.

RDF files can also be read as manifests. Web IRIs, such as ``owl:imports``,
concept schemes and labels missing in the files, are dereferenced only when the
``rdf_fetch_remote`` configuration option is ``true``. It is ``false`` by
default, so web IRIs are not fetched and the result does not depend on the
network.

A concept scheme that no property uses, for example a SKOS file with only a
concept scheme and its concepts, is read as an enum of the main dataset. With
``rdf_fetch_remote`` the concepts are dereferenced, which gives them titles,
descriptions and notations, for example:

.. code-block:: bash

   curl https://publications.europa.eu/resource/authority/atu -o atu.xml
   rdfpipe -i xml -o turtle atu.xml > atu.ttl
   spinta -o rdf_fetch_remote=true copy atu.ttl -o atu.csv

``rdf_fetch_remote`` affects only remote files. Files stored on the disk are
always considered local, so ``file:`` IRIs of ``owl:imports`` are read even when
``rdf_fetch_remote`` is ``false``. An RDF file can therefore make Spinta read
any local RDF file it names in ``owl:imports``, so read only RDF files you
trust:

.. code-block:: bash

   spinta -o rdf_fetch_remote=true copy shapes.ttl -o manifest.csv

.. _cli_udts_oas:

udts oas
========

The ``udts oas`` CLI command exports an OpenAPI 3.0 specification of one UDTS data
service, covering all datasets of that service.

A UDTS data service is identified by the leading part of a dataset path::

   datasets/{form}/{org}/{is}/{service}/{version}/{dataset}/{model}
   └────────────── data service ──────────────┘ └── content ──┘

The generated specification is meant to be used both for importing endpoints
into an API gateway and for validating requests and responses against it, so
``servers`` hold the data service base URL at the API gateway, while ``paths``
are relative to it. The URL is the gateway's, not the agent's: the gateway shows
it in its developer portal, and a single server is given, since the gateway
would use several in turn (round-robin).

Usage
-----

.. code-block:: bash

   spinta udts oas <manifest-files-to-load> -o <path-to-output-file> --path <data-service-path> --udts-cfg <path-to-config-file>

Arguments
---------

- ``<manifest-files-to-load>``
  One or more manifest files that will be loaded.

- ``-o``, ``--output``
  OPTIONAL. Output file name. Specification is written in YAML if the file
  extension is `.yml` or `.yaml`, otherwise in JSON. Without this option the
  specification is written to standard output as JSON.

- ``--path``
  OPTIONAL. Data service path. All datasets of that data service are included:
  the ones equal to it and the ones starting with it followed by ``/``, so
  ``datasets/gov/rc/jadis/at280/1`` does not include
  ``datasets/gov/rc/jadis/at280/10``, and an unversioned
  ``datasets/gov/rc/jadis/at280`` does not include the versioned service either.
  If not given and the manifest holds exactly one data service, that service is
  used, otherwise the command fails listing the data services found.

- ``--udts-cfg``
  REQUIRED, except with ``--list``. YAML file with the information that is not
  part of a manifest: environments, service level ``info`` and the
  authorization server. An example file is shipped as
  ``spinta/manifests/open_api/udts_cfg.example.yml``.
  Three of its fields are required: ``info.title``, the name the data service
  is seen under, ``info.contact`` with every field of it, the ``name``,
  ``url`` and ``email`` of the institution users of the data service turn to,
  and ``servers`` with at least one entry, the first of which an API
  gateway takes the API context path from. A specification without them is not
  written.
  Only a server URL may be relative, every other URL field of the
  configuration has to carry a scheme and a host. A server and
  ``auth.token_url`` are reached over ``https`` or ``http``; ``http`` is
  accepted, a testing deployment for instance, and warned about, since client
  credentials then travel in the clear. ``limits.max_limit`` sets the largest
  ``_limit`` a request may ask for, defaulting to ``100000``.

  Only metadata whose ``visibility`` is ``protected``, ``package`` or
  ``public`` is published. A model, a property or an enum value marked
  ``private``, or given no ``visibility`` at all, which the DSA defaults to
  ``private``, is left out of the specification; a text property is left out
  when none of its languages is published. What is left out is warned about.
  Access to the data is ``access``, not ``visibility``: the service still
  answers with a field the specification does not mention if its ``access``
  allows it.

- ``--api-version``
  OPTIONAL. Value of ``info.version``. Overrides ``info.version`` given in the
  configuration file.

- ``--list``
  OPTIONAL. List data services found in the manifest together with their data
  sets and exit.

Example
-------

**Command**:

.. code-block:: bash

   spinta udts oas manifest.csv -o at280.json \
       --path datasets/gov/rc/jadis/at280/1 \
       --udts-cfg vartai.yml

**Configuration file (`vartai.yml`)**:

.. code-block:: yaml

   info:
     title: JADIS data service
     version: "1"
     contact:
       name: Registrų centras
       url: https://www.registrucentras.lt/
       email: info@registrucentras.lt
   servers:
     - url: https://get.data.gov.lt
       description: Gateway
   auth:
     token_url: https://get.data.gov.lt/auth/token

**Output (`at280.json`)**:

.. code-block:: json

   {
     "openapi": "3.0.3",
     "info": {"title": "JADIS data service", "version": "1"},
     "servers": [
       {"url": "https://get.data.gov.lt/datasets/gov/rc/jadis/at280/1", "description": "Gateway"}
     ],
     "paths": {
       "/version": {},
       "/health": {},
       "/token": {},
       "/at280_israsas/DalyvioAsmensIsrasas": {},
       "/at280_israsas/DalyvioAsmensIsrasas/{id}": {}
     }
   }

**Listing data services**:

.. code-block:: bash

   spinta udts oas manifest.csv --list

.. code-block:: text

   datasets/gov/rc/jadis/at280/1
     datasets/gov/rc/jadis/at280/1/at280_adresai
     datasets/gov/rc/jadis/at280/1/at280_israsas
   datasets/gov/rc/ntr/n249/1
     datasets/gov/rc/ntr/n249/1/n249_israsas
