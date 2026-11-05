.. default-role:: literal

########
Backends
########

Target: ``spinta sync backends``. Status: **Planned**. Parent overview:
:doc:`/contrib/architecture/sync`.

Would synchronize backend configuration (the ``backends`` section of the Spinta
configuration, for example data source and storage connection settings) from the
Metadata Catalogue. Manifest tables are mostly used for data mapping, and
everything else belongs in configuration files. Backend settings are such
configuration, so they are a candidate for this target. No issue exists yet
for this target.

Element
*******

**Planned.** ``spinta sync backends <element>`` takes the backend name, the
``<backend>`` part of ``backends.<backend>.<option>`` in the configuration. For
example, ``default`` for ``backends.default.dsn``.

Location
********

**Planned.**

.. code-block:: text

    $XDG_DATA_HOME/spinta/backends/<backend>.yml

``<backend>`` is the backend name. The file can contain data source
credentials, so it must be readable only by its owner. The design says
"readable only by root", but Spinta normally runs as a regular user, and a
root-only file could not be read by it. The exact permission rule is open, see
`Open questions`_.

Current implementation
**********************

**Planned.** Backend settings are configuration only:

* They are read from the config files (``config`` setting), ``.env``,
  environment variables (for example ``SPINTA_BACKENDS__DEFAULT__DSN``) and
  command line arguments, see :doc:`/contrib/architecture/configuration/index`.
* ``spinta sync`` reads ``backends.default.dsn`` in ``load_configuration_values()``
  (``spinta/cli/helpers/sync/helpers.py``) and does not use the value.
* No sync-owned backend file exists, and nothing writes backend settings.

Open questions
**************

* **Permissions.** The file should be readable only by the owner. Who is the
  owner in practice: the user running ``spinta sync``, or ``root`` as the
  design says?
* **Source of the values.** Does the Catalogue provide credentials at all, or
  only the non-secret connection settings?
* **Priority.** Settings in the user's configuration directory override the
  sync-owned file. Confirm how this combines with environment variables.
