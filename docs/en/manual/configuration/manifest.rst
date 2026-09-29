.. default-role:: literal

.. _manifest-configuration:

Configuring manifests
#####################

Manifest is a schema, where data model served by Spinta is defined. Usually
manifest is a CSV file in tabular DSA format.

Manifest can be specified using one of possible methods:

1. Pass manifest via positional comman line arguments.
2. Pass manifest via configuration.


Positional arguments
********************

One or more manifest files can be passed as positional comman line arguments, for example::

    spinta show manifest.csv

More that one manifest file can be specified, for example::

    spinta show **/*.csv

If manifest file is not specified in command line arguments, for example::

    spinta show

Then, path to manifest file is taken from configuration.


Configuration parameters
************************

In :ref:`config-file`, default manifest is specified in `manifest`
configuration parameter, which points to an entry of `manifests` configuration
parameter, which defines all manifests.

.. code-block:: yaml

    keymaps:
      default:
        type: sqlalchemy
        dsn: sqlite:////path/to/keymap.db

    backends:
      default:
        type: postgresql
        dsn: postgresql://user:pass@host:5432/spinta

    manifest: default
    manifests:
      default:
        type: csv
        path: /path/to/manifest.csv
        backend: default
        keymap: default
        mode: external

In this exaple, `manifest` points to `default` manifest, so `manifests.default`
is used and manifest.
