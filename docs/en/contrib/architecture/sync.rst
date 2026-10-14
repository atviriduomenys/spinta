.. default-role:: literal

######################
Metadata Synchronization
######################

``spinta sync`` synchronizes manifests, clients and other metadata with the
Metadata Catalogue. This command helps keep the local metadata in sync with
the centralized Metadata Catalog, enabling consistent dataset descriptions
across the ecosystem.

Synchronization flows
*********************

The synchronization process follows the architecture defined in
https://github.com/atviriduomenys/katalogas/issues/2302, which describes two-way synchronization
between the Metadata Catalogue and Spinta instances:

1. **Part 1: Catalogue → Agent** (`spinta sync`)
   Fetches the current manifest from the Metadata Catalogue and updates
   the local manifest accordingly.
2. **Part 2: Source → Agent** (TODO: #1489)
   Reflect source schema changes to the local metadata (not yet implemented).
3. **Part 3: Agent → Catalogue** (TODO: #1942)
   Push local manifest changes to the Metadata Catalogue automatically
   when source schema changes (not yet implemented).

The first part is currently functional and can be triggered with the
``spinta sync`` command.

Registry synchronization
***********************

In addition to manifest synchronization, `spinta keymap sync` can synchronize
external identifiers between Spinta instances. Keymaps map external primary
keys to internal identifiers (`_id`, UUIDs). When multiple Spinta instances
publish the same data, keymaps ensure that each row consistently receives
the same internal ID, even when external identifiers change.

The keymap sync command:
* Fetches the changelog ``{server}/{model}/:changes`` from a remote Spinta server
* Converts change events into `KeymapSyncData` entries (insert, update, move mappings)
* Writes the mappings to the keymap database
* Tracks the last synced change ID to avoid redundant syncs

See :doc:`keymap/index` for details on keymap implementations and operations.

`syplo sync` command
********************

The `spinta sync` command performs metadata synchronization:
* Validates the connection to the Metadata Catalogue
* Fetches dataset IDs from the Data Service associated with the Spinta instance
* Syncs manifests by merging them, with catalogue fields taking precedence
* Goal is to keep the local manifest up-to-date with the catalog's definition

The main implementation lives in
``spinta/cli/sync.py`` with helper controllers in
``spinta/cli/helpers/sync/``:

* ``api_helpers`` — API communication utilities
* ``controllers/data_service.py`` — dataset discovery
* ``controllers/synchronization/connection.py`` — connection validation
* ``controllers/synchronization/manifest_catalog_to_agent.py`` — manifest merging logic
* ``helpers.py`` — context preparation and manifest loading

Implementation details
***********************

The manifest synchronization follows a hierarchical merge strategy:

.. code-block:: text

    Manifest (datasets)
    ├── Resources (per dataset)
    │   └── Models (per resource)
    │       └── Properties (per model)

Each level performs a merge with the following rules:

* **Identical manifests** — skips synchronization and reports they match
* **Empty local manifest** — writes the catalog manifest locally
* **Different manifests** — merges fields, with catalogue taking precedence

Fields to merge per entity type are defined in
``spinta/cli/helpers/sync/controllers/synchronization/``:

* `FIELDS_TO_MERGE_FOR_DATASET`
* `FIELDS_TO_MERGE_FOR_RESOURCE`
* `FIELDS_TO_MERGE_FOR_MODEL`
* `FIELDS_TO_MERGE_FOR_PROPERTY`

Reconciliation logic (``find_existing_entity``):
* Checks for existing entity by matching ``id`` and/or ``name`` attributes
* Updates existing entities with new catalogue fields
* Sets renamed entities using ``commands.set_<entity>()`` helpers

File storage
***********

The synchronized manifest is written back to the local manifest file,
ensuring the local copy reflects the catalog's current state.

Status and roadmap
******************

The main ``spinta sync`` flow is partially implemented (Part 1 complete,
Parts 2 and 3 TODO). Related issues track the remaining work:

* **Part 2: Source → Agent**: https://github.com/atviriduomenys/spinta/issues/1489
* **Part 3: Agent → Catalogue**: https://github.com/atviriduomenys/katalogas/issues/1942

See the architecture issue for the high-level design:
https://github.com/atviriduomenys/katalogas/issues/2302

.. seealso::

    * :doc:`keymap/index` — Keymap synchronization and identifier mapping
    * :doc:`/contrib/architecture/data-access/changes` — The ``:changes`` endpoint used for sync
    * :doc:`user-interfaces/cli` — CLI commands and usage
    * :doc:`/contrib/architecture/manifests/index` — Manifest format and structure
