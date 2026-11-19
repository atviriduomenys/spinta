.. default-role:: literal

########
Datasets
########

Target: ``spinta sync datasets``. Status: **Planned**. Parent overview:
:doc:`/contrib/architecture/sync`.

This target synchronizes **dataset-level parameters** from the Metadata
Catalogue to the agent, for example how often the dataset is updated. It is
planned as its own target so that jobs which need only these parameters (for
example the changelog compression job) do not have to run a full
``spinta sync``.

Purpose
*******

Some agent jobs need a dataset parameter that is not part of the manifest:

* **Changelog compression** (spinta#1717). The compression job keeps changelog
  records for a retention period. The period is stored in the catalogue and
  the agent must read it. The job runs periodically (cron), so it must not
  trigger a full metadata synchronization each time (spinta#2086).
* **Update periodicity.** The catalogue stores how often a dataset is updated.
  This is the DCAT property ``dct:accrualPeriodicity``. A job that checks
  freshness or decides when to run can use it.

Naming of the parameter is not settled, see `Open questions`_.

Element
*******

**Planned.** ``spinta sync datasets <element>`` takes the dataset code name, as
it is written in the ``dataset`` column of the manifest, for example
``datasets/gov/rc/ar``.

Without an element, the target processes every dataset that belongs to the
agent's Data Service.

Location
********

**Planned.** Each dataset has its own sync-owned file:

.. code-block:: text

    $XDG_DATA_HOME/spinta/sync/<catalog>/datasets/<dataset>.yml

``<dataset>`` is the dataset code name. For ``datasets/gov/rc/ar`` the file is
``~/.local/share/spinta/sync/katalogas/datasets/datasets/gov/rc/ar.yml``. The ``datasets/``
segment appears twice, because the dataset code name already starts with
``datasets/``. The rule keeps it, see `Open questions`_.

The :doc:`models` target writes into the same file. Each file holds the
settings of one dataset and of all models that belong to it. A dotted key
replaces only the item it names, so the file for one dataset does not reset the
settings of other datasets (see :ref:`configuration-syntax`):

.. code-block:: yaml

    datasets.datasets/gov/vpk:
      resource: sql://user:pass@host/db
    models.datasets/gov/vpk/Data:
      backend: default

Users must not edit this file. To change a value, the user sets it in the
configuration directory ``$XDG_CONFIG_HOME/spinta``. That value has priority
over the file (see :doc:`/contrib/architecture/sync`).

Where the values live today
***************************

Catalogue
    The catalogue dataset model (``vitrina/datasets/models.py`` in the
    Katalogas repository) has the field ``frequency``, a foreign key to a
    ``Frequency`` classifier, with the help text "Duomenų atnaujinimo dažnumas.
    Atitinka dct:accrualPeriodicity." (update frequency, corresponds to
    ``dct:accrualPeriodicity``). There is also a free text field
    ``update_frequency``. The API representation used by the agent is not
    verified yet.

Spinta configuration (``config.yml``)
    The top-level ``datasets`` key holds parameters for datasets. Each dataset
    is a dotted key, named by its object code name, so that one setting can be
    overridden without resetting the others. Model settings are under the
    ``models`` key, see :doc:`models`. The intended shape is:

    .. code-block:: yaml

        datasets.gov/vpk:
          resource: sql://user:pass@host/db

    The value is loaded into ``Config.datasets`` (``spinta/types/config.py``).
    The comment says that datasets should read their own parameters directly.
    No command in this repository reads a periodicity or retention value from
    it today.

Manifest
    The dataset fields that ``spinta sync`` merges are listed in
    ``FIELDS_TO_MERGE_FOR_DATASET``. They do not include any periodicity field,
    so the value is not carried over today.

Existing building blocks
************************

These parts already exist and can be reused:

* ``validate_credentials()`` and ``connection_check()`` — credentials and
  connection check (``spinta/cli/helpers/sync/``).
* ``get_data_service_children_dataset_ids()`` — the list of dataset ids that
  belong to this agent's Data Service (``spinta/cli/helpers/sync/controllers/data_service.py``).
* ``get_resources()`` — generic ``GET /Dataset/`` query with parameters
  (``spinta/cli/helpers/sync/controllers/dataset.py``). It is used today to
  find Data Services and datasets by ``name`` and ``parent_id``.
* Multiple datasets per run are already handled by a loop in the manifests
  target (see :doc:`manifests`).

Proposed command
****************

.. code-block:: text

    spinta sync datasets                        # all datasets of this agent
    spinta sync datasets datasets/gov/rc/ar     # one dataset

Like all targets, it runs only when due (see the scheduling section in
:doc:`/contrib/architecture/sync`).

Proposed flow
=============

1. Check credentials and connection to the Catalogue.
2. Find the agent's Data Service and its child datasets. If an element is
   given, only that dataset is processed.
3. For each dataset, read the dataset-level parameters from the Catalogue.
   **Not implemented:** a request that returns a single dataset with these
   fields. The existing helpers only list datasets by ``name`` or ``parent_id``.
4. Compare the values with the values stored by the previous run.
5. If anything changed, rewrite the dataset's sync-owned file (see
   `Location`_). Record the run in the sync state store, with its time and
   status.
6. Print one summary line per dataset: changed, unchanged or failed.

Consumers, such as the compression job, read the values from configuration.
They must not call the Catalogue themselves.

Output: where the values are written
************************************

**Planned.** The values are written to one sync-owned file per dataset, under
``$XDG_DATA_HOME/spinta/sync/<catalog>/datasets/`` (see `Location`_). The options considered
were:

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Option
     - Advantages
     - Disadvantages
   * - ``spinta sync datasets -o <config.yml>``
     - Explicit output path.
     - Partners must know the path. ``-o`` is already the root option for
       configuration overrides (``spinta -o option.name=value``). Writing into
       the user's ``config.yml`` mixes sync output with hand-written settings.
   * - One sync-owned file in the configuration directory (for example
       ``$XDG_CONFIG_HOME/spinta/datasets.yml``)
     - No path to configure.
     - The configuration directory is for user-edited files. Sync output there
       would be mixed with user settings and could be edited by hand.
   * - **One sync-owned file per dataset in** ``$XDG_DATA_HOME/spinta/sync/<catalog>/datasets/``
       **(chosen)**
     - No path to configure. The user can override any value in the
       configuration directory. The sync output is never edited by hand. Each
       dataset is separate, so one failed dataset does not block the others.
     - Needs a load order rule (sync-owned files have the lowest priority, see
       :doc:`/contrib/architecture/sync`). The existing ``config`` key loads extra
       configuration files after the defaults (see
       :doc:`/contrib/architecture/configuration/index`), but it does not load
       files from the data directory, so this part is not implemented.
   * - Standard output only
     - Simple, nothing to manage.
     - Nothing is stored, so jobs cannot read the values later.

The same rule applies to ``spinta sync`` without a subcommand. It runs the
targets in turn and prints one summary per target. It does not print the
values of all targets to stdout.

Behaviour when values are missing
=================================

Not decided yet. Possible rules:

* the Catalogue has no value: keep the previous value, or remove it;
* the Catalogue is not reachable: keep the previous value and record the error
  in the sync state store.

Current status
**************

**Planned.** Nothing of the target exists in code:

* no ``datasets`` target in ``spinta sync``;
* no element argument, ``spinta sync`` takes no arguments today;
* no request that reads a single dataset's periodicity or retention value;
* no sync-owned file under ``$XDG_DATA_HOME/spinta/sync/<catalog>/datasets/``;
* no reader of the value in the compression job;
* no sync state store.

Open questions
**************

* **Parameter name.** Use the DCAT name where the semantics match, and another
  name where they do not.

  * **Update frequency: keep** ``accrual_periodicity``. DCAT defines
    ``dct:accrualPeriodicity`` as "the frequency at which a dataset is
    published", with range ``dcterms:Frequency`` ("a rate at which something
    recurs"). The catalogue field ``frequency`` ("atitinka
    dct:accrualPeriodicity") has the same meaning, so the DCAT name fits.
  * **Retention: different semantics, so a different name.** The retention
    value is not a recurrence rate. It is a length of time: how long changelog
    records are kept before they are compressed or removed (spinta#1717, default
    6 months). The value is a duration, not a classifier entry like
    ``Frequency``. ``retention_periodicity`` (spinta#2086) is misleading for the
    same reason, because "periodicity" means recurrence. DCAT 3 has no
    retention property, so the name is free. Proposed: ``retention_period``,
    holding an ISO 8601 duration (``xsd:duration``, the type DCAT uses for
    ``dcat:temporalResolution``). If the value applies only to changelog,
    ``changelog_retention_period`` is more exact. This choice must be made
    before implementation.

  Note that the issue title of spinta#1717 says "retention policy". The
  parameter name is not ``retention_policy`` in either issue.
* **Value format.** ``frequency`` is a classifier (``Frequency``), so the agent
  must know whether it gets the classifier code or the text. A retention value
  is a duration, such as ``P6M``, so it needs its own format decision. The
  catalogue field for retention is not documented on this page.
* **File format.** The location is a ``.yml`` file with a top-level
  ``datasets`` key (see `Location`_). The parameter names inside it, and how
  they are loaded as configuration, are not decided.
* **Doubled** ``datasets/`` **segment.** The location rule gives
  ``datasets/datasets/gov/rc/ar.yml``. Should the leading ``datasets/`` of the
  code name be dropped from the file path and from the keys in the file? The
  examples in this page use keys without it, for example ``gov/vpk``.
* **Behaviour when values are missing or the Catalogue is unreachable** (see
  above).
* **Whether other dataset parameters belong to this target**, for example
  access level or versions (see spinta#1647 for versions).

Related issues
**************

* spinta#2086 — "Duomenu naujinimo parametro isgavimas": the request for this
  target.
* spinta#1717 — changelog retention policy, which uses the parameter.
* spinta#1404 — multiple datasets in one run (closed; the loop exists, see
  above).
* spinta#1647 — versioning, which affects dataset-level fields.
* katalogas#1584 — daemon mode for ``spinta sync``.

.. seealso::

    * :doc:`/contrib/architecture/sync` — target architecture and status
    * :doc:`manifests` — the target that already works (Part 1)
    * :doc:`models` — model-level configuration, same file location
    * :doc:`/contrib/architecture/configuration/index` — how configuration files are read
