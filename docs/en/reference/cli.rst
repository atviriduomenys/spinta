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
