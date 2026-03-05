.. default-role:: literal

.. Spinta documentation master file, created by
   sphinx-quickstart on Wed Jan  8 12:01:54 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Spinta documentation!
=====================

Spinta is a tool for managing open and not necessarily open data. It consists
of these main components:

command line tool:
   A command line tool `spinta`, for managing data and metadata.

data publishing server:
   An HTTP server, with an API for publishing data.

These tools work together using same metadata describing data. Metadata is
compatible with DCAT standard, but extended to support describing data at
individual column level.

Metadata format is a simple table consisting of 5 dimension and 10 metadata
columns.

You can open data following these simple steps:

1. Describe your dataset.

   You can automatically inspect your data source using this command::

      spinta inspect sqlite:////data.db -o manifest.xlsx

   This will create a file `manifest.xslx`.

   Then you need to open `manifest.xlsx` using your favorite spreadsheet
   program and fill in missing metadata, specifying which columns you want to
   open.

   While describing your data you can preview how things would look, before
   publishing anything::

      spinta run --mode external manifest.xlsx

   This will read data directly from the data source (`--mode external`) and
   will run a local data publishing server.

2. Publish your dataset.

   When you have your dataset fully described, you can publish you data into
   a data publishing server (assuming you have one up and running)::

      spinta push manifest.xlsx -o https://client@data.example.com

   For this command to work, you need to add client credentials to
   `~/.config/spinta/credentials.cfg` file.

Opening and publishing your data has never been so simple!


.. toctree::
   :maxdepth: 2
   :caption: Manual

   manual/setup/index
   manual/configuration/index
   manual/access
   manual/manifest/index
   manual/functions
   manual/migrations
   manual/accesslog

.. toctree::
   :maxdepth: 2
   :caption: Reference

   reference/components
   reference/commands
   reference/cli

.. toctree::
   :maxdepth: 2
   :caption: Contributing

   contrib/index


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
