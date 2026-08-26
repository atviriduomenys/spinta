.. default-role:: literal

##########
CLI
##########

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
  OPTIONAL. Output file name. If specified, manifest will be written to this file. If file extension is `.mmd`, UML diagram will be generated in Mermaid syntax.

- ``<dataset-name>``
  The name of the dataset. This is used only for Mermaid diagram. Specified dataset will not be wrapped in Mermaid class diagram `namespace <https://mermaid.js.org/syntax/classDiagram.html#define-namespace>`_

Example
-------

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

.. _cli_udts_oas:

udts oas
========

The ``udts oas`` CLI command exports an OpenAPI specification of one UDTS data
service, covering all data sets of that service.

A UDTS data service is identified by the leading part of a data set path::

   datasets/{form}/{org}/{is}/{service}/{version}/{dataset}/{model}
   └────────────── data service ──────────────┘ └── content ──┘

The generated specification is meant to be used both for importing endpoints
into an API gateway and for validating requests and responses against it, so
``servers`` hold the data service base URL of every environment, while ``paths``
are relative to it.

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
  OPTIONAL. Data service path. All data sets of that data service are included:
  the ones equal to it and the ones starting with it followed by ``/``, so
  ``datasets/gov/rc/jadis/at280/1`` does not include
  ``datasets/gov/rc/jadis/at280/10``, and an unversioned
  ``datasets/gov/rc/jadis/at280`` does not include the versioned service either.
  If not given and the manifest holds exactly one data service, that service is
  used, otherwise the command fails listing the data services found.

- ``--udts-cfg``
  OPTIONAL. YAML file with the information that is not part of a manifest:
  environments, service level ``info`` and the authorization server. An example
  file is shipped as ``spinta/manifests/open_api/udts_cfg.example.yml``.
  Only a server URL may be relative, every other URL field of the
  configuration has to carry a scheme and a host, and ``auth.token_url`` has to
  use HTTPS.

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
   servers:
     - url: https://get.data.gov.lt
       description: Production
     - url: https://test-get.data.gov.lt
       description: Testing
   auth:
     token_url: https://get.data.gov.lt/auth/token

**Output (`at280.json`)**:

.. code-block:: json

   {
     "openapi": "3.1.0",
     "info": {"title": "JADIS data service", "version": ""},
     "servers": [
       {"url": "https://get.data.gov.lt/datasets/gov/rc/jadis/at280/1", "description": "Production"},
       {"url": "https://test-get.data.gov.lt/datasets/gov/rc/jadis/at280/1", "description": "Testing"}
     ],
     "paths": {
       "/:version": {},
       "/:token": {},
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
