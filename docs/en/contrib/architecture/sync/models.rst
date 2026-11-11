.. default-role:: literal

######
Models
######

Target: ``spinta sync models``. Status: **Planned**. Parent overview:
:doc:`/contrib/architecture/sync`.

Would synchronize model-level settings. Model structure and data mapping stay in
manifest tables. Some model settings can already be changed in configuration, see
`Current model configuration`_. The Metadata Catalogue does not provide any
model settings today, see `Sources of model settings`_.

Element
*******

**Planned.** ``spinta sync models <element>`` takes the full model code name. It
is the dataset code name from the ``dataset`` column combined with the model
name from the ``model`` column, for example ``datasets/gov/rc/ar/City``.

Current model configuration
***************************

Model settings are read from the ``models`` configuration key. Each model has
one entry, keyed by the full model code name:

.. code-block:: yaml

    models:
      datasets/gov/example/City:
        backend: default
        distribute:
          type: copy
        properties:
          population:
            type: sqlalchemy.types.BigInteger
          history:
            type:
              name: sqlalchemy.types.String
              length: 200

The following keys are supported:

``backend``
    Name of a backend from ``backends``. The model is stored in that backend.

``distribute``
    Distribution strategy of the model, written as a mapping. ``type`` is one of
    ``schema``, ``table``, ``copy`` or ``undistributed``. ``type: table`` also
    needs ``property``, the name of the property used as distribution key. The
    shorthand ``distribute: copy`` is not supported.

``properties.<name>.type``
    Custom column type of the property. It is a dotted path to a Python type,
    for example ``sqlalchemy.types.BigInteger``. To pass arguments to the type,
    write a mapping with ``name`` and the arguments, as in ``history`` above.

``properties.<name>.backend``
    Backend of the property.

Settings are read with the usual configuration rules, see
:ref:`configuration-syntax`. A dotted key changes one setting and keeps the
rest of the model entry, for example ``models.datasets/gov/example/City.distribute.type``
changes only the distribution type.

Configuration is checked when the manifest is checked:

* a configured model that does not exist in the manifest raises
  ``ModelNotFound``;
* a configured property that does not exist in the model raises
  ``PropertyNotFound``;
* a ``table`` distribution without ``property`` raises
  ``MissingConfigurationParameter``; an unknown distribution type raises an
  error.

Implementation: ``spinta/types/model.py`` (``configure`` for ``Model`` and
``Property``, ``check`` for ``Model``).

Sources of model settings
*************************

A model setting can come from three places. They differ in who owns the value,
and that decides whether the value is sent to the Catalogue.

User configuration
    Set by the user in ``config.yml``, environment variables or command line
    arguments. This is the only source that exists today. A user value has
    priority over the sync-owned file (see `Location`_).

Metadata Catalogue
    **Not provided today.** The Catalogue does not store any model options for
    this target. If it starts to provide some, the values are received by this
    target and written to the dataset's sync-owned file under ``models``. They
    are owned by the Catalogue, so changes made by the agent to them are
    expected to be sent back by a future Part 3 (see :doc:`manifests`).

Data source
    Values inferred from the external data source, for example by reading the
    table size. The data source is read by Part 2 (Source to Agent, see
    :doc:`manifests`). Such values are written to the same sync-owned file, but
    they are owned by the agent. They are never sent to the Catalogue, and they
    are never received from it, unless the Catalogue starts to manage that
    setting.

An example of a data source setting is the row limit proposed in spinta#1491.
The limit of a model is calculated from the size of its table, so it can be
inferred from the data source. The issue discusses keeping it in a separate
``limits.yml`` in the configuration directory, keyed by model name. That file is
not implemented in this repository. If the limit is kept in the sync-owned file
instead, it would be written under the model's key:

.. code-block:: yaml

    models.datasets/gov/rc/ar/City:
      limit: 10000

The key name and the file are not decided, see `Open questions`_.

Current implementation
**********************

**Planned.** The sync target does not exist yet:

* no ``models`` target in ``spinta sync``;
* no request that returns model-level settings from the Catalogue, because the
  Catalogue has none;
* model configuration is read only from the user configuration sources listed
  in `Current model configuration`_. Nothing writes it from a sync run;
* the ``config`` option does not load configuration files that are named inside
  another configuration file. Model settings kept in separate files, as
  proposed in spinta#1990, therefore cannot be loaded yet (see spinta#1991);
* the row limit from spinta#1491 is not in this repository;
* model structure is only in manifest tables, which the :doc:`manifests` target
  synchronizes.

Location
********

**Planned.** Model settings are stored in the sync-owned file of the dataset
that the model belongs to:

.. code-block:: text

    $XDG_DATA_HOME/spinta/sync/<catalog>/datasets/<dataset>.yml

For ``datasets/gov/rc/ar/City`` the dataset is ``datasets/gov/rc/ar``, so the
file is ``~/.local/share/spinta/sync/katalogas/datasets/datasets/gov/rc/ar.yml``. The same file
is used by the :doc:`datasets` target.

The file holds the settings of the dataset and of all its models. A model key
is the dataset name followed by the model name:

.. code-block:: yaml

    datasets.datasets/gov/rc/ar:
      resource: sql://user:pass@host/db
    models.datasets/gov/rc/ar/City:
      backend: default
      distribute:
        type: copy
      properties:
        population:
          type: sqlalchemy.types.BigInteger

A dotted key replaces only the item it names, so setting one model does not reset
the other models or the dataset itself (see :ref:`configuration-syntax`).

The ``models`` keys in the file are the keys that the configuration reads for
models, so the file is used in the same way as user configuration. Because the
file has the lowest priority, a user value always overrides it.

Open questions
**************

* **Catalogue model options.** The Catalogue provides none today. Which options
  it should provide, and under which field names, is not decided.
* **Row limit location.** Keep the limit in ``$CONFIG/limits.yml`` as discussed
  in spinta#1491, or move it to ``models.<code>.limit`` in the sync-owned file.
  If it moves, the key name must be declared in the ``models`` configuration
  schema.
* **Several writers of one file.** The Catalogue-owned and the agent-owned
  values share the per-dataset file. A run of one writer must not remove values
  that the other writer wrote. Either the writers own separate keys, or they use
  separate files.
* **Loading the file.** The sync-owned file takes effect only when it is loaded
  as a configuration source. This needs nested configuration loading (spinta#1991).

Related issues
**************

* spinta#1990 — nested `config` loading and model configuration in separate
  files. The issue gives the `models` example used above.
* spinta#1991 — pull request for nested configuration files.
* spinta#1491 — limit on the number of rows returned for a model, which is
  inferred from the data source.
* spinta#599 — custom property types, configured under `models.<name>.properties`.
* spinta#1691 — `distribute` for models, configured under `models.<name>.distribute`.
* spinta#649 ("Struktūros aprašų sinchronizavimas"), which describes
  synchronizing structure descriptions in general. No dedicated issue exists
  yet for this target.

.. seealso::

    * :doc:`/contrib/architecture/sync` — target architecture and status
    * :doc:`datasets` — the dataset-level target that shares the file location
      and the key layout
    * :doc:`manifests` — Part 2 (Source to Agent) and Part 3 (Agent to Catalogue)
    * :doc:`/manual/configuration/index` — configuration sources and syntax
