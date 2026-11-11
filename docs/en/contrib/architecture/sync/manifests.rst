.. default-role:: literal

#########
Manifests
#########

Target: ``spinta sync manifests``. Status: **Partial**. Parent overview:
:doc:`/contrib/architecture/sync`.

Synchronizes the data structure tables (manifests) of the agent's datasets with
the Metadata Catalogue. Part 1 (Catalogue to Agent) is implemented: the agent
downloads the DSA of its datasets and merges it into the local manifest. Part 2
(reading the external source schema) and Part 3 (sending local changes back to
the Catalogue) are planned.

Element
*******

**Planned.** ``spinta sync manifests <element>`` takes the path to a manifest
file, for example ``path/to/manifest.csv``.

Open question: the element is not yet defined as an input or as an output. The
local manifest is the file that is merged, and it is currently the file set in
``manifests.default.path``. See `Open questions`_.

Location
********

**Planned.** Each dataset has its own sync-owned manifest file:

.. code-block:: text

    $XDG_DATA_HOME/spinta/sync/<catalog>/manifests/<dataset>.csv

``<dataset>`` is the value of the ``dataset`` column, for example
``datasets/gov/example``. The file is then
``~/.local/share/spinta/sync/katalogas/manifests/datasets/gov/example.csv``.

One file per dataset fits the catalogue API, because Part 1 downloads one DSA
per dataset id.

Current implementation
**********************

**Partial.** Part 1 works today. The steps are:

1. Read the ``katalogas`` section of ``credentials.cfg`` (falls back to the old
   ``default`` section). Validate that required values are present.
2. Get an access token and check the connection to the Catalogue. The check
   sends the Spinta version (``connection_check``).
3. Find the Data Service that belongs to this agent. The agent name is the
   client name without its suffix (``get_agent_name``). Then list the child
   datasets of that Data Service (``get_data_service_children_dataset_ids``).
4. For each dataset, download its structure description (DSA) with
   ``GET /Dataset/{id}/dsa`` (``get_dsa``) and read all of them as one catalogue
   manifest.
5. Compare the catalogue manifest with the local manifest at
   ``manifests.default.path``:

   * identical manifests — nothing is written, the local manifest is printed;
   * empty local manifest — the catalogue manifest is written as the local one;
   * different manifests — fields are merged, catalogue values take precedence.

6. Write the merged manifest back to ``manifests.default.path`` and print it.

Differences from the target design:

* The file is one local manifest for all datasets, set by the user in
  ``manifests.default.path``. The target design uses one sync-owned file per
  dataset under ``$XDG_DATA_HOME/spinta/sync/<catalog>/manifests/``, and ignores
  ``manifests.default.path`` except as an override.
* The element argument is not accepted, the command takes no arguments.
* The whole manifest is printed to stdout instead of a summary.
* ``load_configuration_values()`` also reads ``backends.default.dsn``, which
  the source step (Part 2) would need. ``sync()`` currently ignores it.

Code locations:

* ``spinta/cli/sync.py`` — the ``sync`` command.
* ``spinta/cli/helpers/sync/`` — helpers: ``helpers.py`` (configuration,
  credentials, context and manifest loading), ``api_helpers.py`` (API calls)
  and ``controllers/`` (datasets, Data Service, DSA, synchronization).
* ``spinta/cli/helpers/sync/controllers/synchronization/manifest_catalog_to_agent.py``
  — manifest merging logic.

Merge rules
===========

The merge works on the same hierarchy as the manifest:

.. code-block:: text

    Manifest (datasets)
    ├── Dataset
    │   └── Resource
    │       └── Model
    │           └── Property

For each level, the existing local entity is found by the first matching key
(``find_existing_entity``). Datasets are matched by ``id``, ``name`` or
``source``; resources by ``id``, ``name`` or ``external``; models and
properties by ``id``, ``name`` or ``external.name``. Matching entities are
updated with the catalogue value of each field
listed in ``FIELDS_TO_MERGE_FOR_DATASET``, ``FIELDS_TO_MERGE_FOR_RESOURCE``,
``FIELDS_TO_MERGE_FOR_MODEL`` and ``FIELDS_TO_MERGE_FOR_PROPERTY``
(``spinta/cli/helpers/sync/controllers/synchronization/__init__.py``). When the
catalogue renames an entity, the local entity is renamed too. Entities that do
not exist locally are added.

Fields that are not in these lists are not synchronized. For example, the
dataset update periodicity is not in ``FIELDS_TO_MERGE_FOR_DATASET``, so it is
not carried over today (see :doc:`datasets`).

Helper code that is present but not used
========================================

The following helpers exist in ``spinta/cli/helpers/sync/controllers/`` but no
command calls them yet:

* ``create_dsa`` (``POST /Dataset/{id}/dsa/``) — first upload of a local DSA.
* ``update_dsa`` — raises ``NotImplementedFeature``, waiting for
  katalogas#1600.
* ``create_resource`` (``POST /Dataset/``) and ``create_distribution``
  (``POST /Distribution/``) — creating catalogue objects for an agent.

These belong to Part 3 (Agent to Catalogue).

Synchronization parts
=====================

The high-level design is defined in katalogas#2302. The three parts are:

.. list-table::
   :header-rows: 1
   :widths: 16 34 14 36

   * - Part
     - Direction
     - Status
     - Issues
   * - Part 1
     - Catalogue to Agent: fetch the manifest of the agent's datasets from
       the Catalogue and merge it into the local manifest.
     - Implemented
     - #1488 (done), #1647 (versioning, open)
   * - Part 2
     - Source to Agent: read the external source schema (``spinta inspect``)
       and reflect changes in local metadata.
     - Planned
     - #1489
   * - Part 3
     - Agent to Catalogue: send local manifest changes to the Catalogue
       when source schema changes.
     - Planned
     - katalogas#1942, katalogas#1600 (DSA update endpoint), spinta#1378
       (closed, first DSA export)

Other pieces that are not wired into ``spinta sync``:

* ``spinta inspect`` already reads an external source and updates a manifest,
  but it is a separate manual command.

Tests
=====

``tests/test_sync.py`` covers credentials handling, the connection check, the
behaviour when the Data Service is missing or has no datasets, and the Part 1
merge cases. HTTP calls are mocked with ``requests_mock``.

Missing parts and issues
========================

* **Predefined per-dataset location** under ``$XDG_DATA_HOME/spinta/sync/<catalog>/manifests/``,
  instead of ``manifests.default.path``. No issue yet.
* **Element argument** (``spinta sync manifests <element>``). No issue yet.
* **Part 2, source to agent** — spinta#1489.
* **Part 3, agent to catalogue** — katalogas#1942, katalogas#1600.
* **Versioning (several MAJOR versions per dataset)** — spinta#1647.
* **Summary output** instead of printing the whole manifest. No issue yet.

Open questions
==============

* **Element meaning.** Is the path given to ``spinta sync manifests`` the local
  manifest that is merged (input), or the file where the result is written
  (output)? The predefined location is per dataset, so a single manifest file
  with several datasets would have to be split.
* **Relation to** ``manifests.default.path``. Should the setting stay as an
  override only, as the design says, or be removed?

Related issues
==============

* spinta#1488 — Part 1, done.
* spinta#1489 — Part 2, source to agent.
* katalogas#1942, katalogas#1600 — Part 3, agent to catalogue.
* spinta#1647 — versioning.

.. seealso::

    * :doc:`/contrib/architecture/sync` — target architecture and status
    * :doc:`datasets` — dataset-level parameters
    * :doc:`/contrib/architecture/manifests/index` — manifest format and structure
