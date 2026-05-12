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
