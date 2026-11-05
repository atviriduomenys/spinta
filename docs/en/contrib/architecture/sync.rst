.. default-role:: literal

########################
Metadata Synchronization
########################

``spinta sync`` synchronizes metadata between a Spinta instance (the *agent*)
and the Metadata Catalogue (Katalogas). It keeps local manifests, configuration
and other metadata consistent with the centralized catalogue, so that dataset
descriptions stay the same across the ecosystem.

This page describes the **target architecture** of ``spinta sync`` and, for
each part, what is implemented today and what is still missing. Missing parts
point to GitHub issues. Each synchronization target has its own page, with the
details of its element syntax, file locations and current implementation, see
`Synchronization targets`_ below.

Status markers used on this page and on the target pages:

* **Implemented** — works in the code base today.
* **Partial** — some pieces exist, others are missing.
* **Planned** — only the design exists, nothing is implemented.

What ``spinta sync`` is not
***************************

``spinta sync`` is **not** a migration command, does **not** move data rows
and is **not** keymap synchronization. Each of these is handled by other
commands:

* ``spinta bootstrap`` — creates missing schemas and tables in internal
  backends. It does not alter existing tables, columns or indexes.
* ``spinta migrate`` — applies schema changes to existing internal tables and
  creates new tables (see :doc:`data-access/backends/internal-data-store`).
* ``spinta push`` and ``spinta pull`` — move data rows between Spinta and
  external stores or remote Spinta instances.
* ``spinta import`` and ``spinta export`` — read and write data files.
* ``spinta keymap sync`` — synchronizes external identifiers between Spinta
  instances (see :doc:`keymap/index`). It is also run internally by
  ``spinta push`` and ``spinta export``.

Target architecture
*******************

Audience
========

``spinta sync`` is meant to be the single thing that users need to know
about metadata. They should not need to know the details of each target.
Users run ``spinta sync`` and the command takes care of everything that is
configured for their instance.

Command model
=============

**Planned.** The command model follows the style of ``spinta upgrade``, which
accepts optional script names and runs all of them when none are given.

.. code-block:: text

    spinta sync                       # all targets, in a fixed order
    spinta sync <target>              # one target, e.g. spinta sync datasets
    spinta sync <target> <element>    # one element of a target, e.g. one dataset

The syntax of ``<element>`` depends on the target. Each target page defines its
own element, for example a manifest file path for :doc:`sync/manifests` or a
dataset code name for :doc:`sync/datasets`. Today ``spinta sync`` takes no
arguments at all (see `Current implementation`_).

Targets
=======

Synchronization is split into targets. Each target has a direction (what is
read from where and written to where) and a predefined effect on local files
or on the catalogue.

.. list-table::
   :header-rows: 1
   :widths: 14 36 16 34

   * - Target
     - Purpose
     - Status
     - Issues
   * - :doc:`sync/manifests`
     - Data structure (manifest tables) from Catalogue, schema from external
       sources, and back to Catalogue.
     - Partial
     - #1488, #1489, #1942, #1647
   * - :doc:`sync/backends`
     - Backend configuration (``backends`` in config).
     - Planned
     - None yet
   * - :doc:`sync/datasets`
     - Dataset-level parameters, for example update periodicity.
     - Planned
     - #2086, #1717
   * - :doc:`sync/models`
     - Model-level configuration.
     - Planned
     - #649 (related)
   * - :doc:`sync/contracts`
     - Smart contracts (ODOC / ``adoc`` documents) from Catalogue.
     - Planned
     - #1684, #2071 (related)
   * - :doc:`sync/clients`
     - OAuth clients, if Catalogue manages client management.
     - Partial
     - #1656 (closed), #1684
   * - :doc:`sync/logs`
     - Aggregated access log statistics sent to Catalogue.
     - Planned
     - Katalogas #298, #1057 (related)
   * - :doc:`sync/push`
     - ``spinta push`` state (last push date, errors) sent to Catalogue.
     - Planned
     - #1416

New targets can be added later if needed.

Sync-owned files and locations
==============================

**Planned.** ``spinta sync`` always uses predefined locations, so that it knows
where to find and update its files. The rules are the same for every target:

* All sync-owned files are stored under ``$XDG_DATA_HOME/spinta/`` (by default
  ``~/.local/share/spinta``). Each target has its own directory there, for
  example ``manifests/``, ``datasets/`` or ``clients/``. The exact file pattern
  of each target is given on its target page. The state of each Catalogue
  is in ``sync/<catalog>/``, a state file and a lock file per target, see
  `Scheduling and state tracking`_.
* Users cannot change individual file locations. Only ``XDG_DATA_HOME`` moves
  the whole tree.
* Settings that point to a file, for example ``manifests.default.path``, are
  only overrides. Otherwise they are ignored by ``spinta sync``.
* Users can override values from ``$XDG_CONFIG_HOME/spinta`` (by default
  ``~/.config/spinta``). Sync-owned files have the lowest priority, so any value
  set by the user wins over the value in a sync-owned file.
* Overrides work per element. A user can change one value of one element, for
  example the update periodicity of a single dataset, and all other elements
  keep the values from their sync-owned files. The override is set in the
  configuration directory and is keyed by the element. Each target page defines
  its element, for example :doc:`sync/datasets`. The key layout of the
  overrides depends on the target, and each target page describes its own.
* Sync-owned files must not be edited by hand. ``spinta sync`` rewrites them.
* Some sync-owned files can contain credentials (for example data source
  credentials of a backend). Such files must have restricted file permissions,
  see the target page.

Consequences of this design:

* No output path option (``-o``) is planned for ``spinta sync`` targets, target
  files have predefined locations.
* Each element writes to its own file (for example one file per dataset), as
  defined by its target page. There is no need for several output paths per
  target.
* Without a subcommand, ``spinta sync`` prints a short summary per target
  (changed, unchanged, failed), not the whole manifest. Full results can be
  found in the sync-owned files and in the log.

Current behaviour differs. Today ``spinta sync`` writes the merged manifest to
the path configured in ``manifests.default.path`` and prints the whole manifest
to stdout (see `Current implementation`_).

The code base already has the default directories. ``DEFAULT_DATA_PATH`` and
``DEFAULT_CONFIG_PATH`` in ``spinta/core/config.py`` are built from
``XDG_DATA_HOME`` and ``XDG_CONFIG_HOME``. The ``data_path`` and ``config_path``
settings can still move these directories, which the target design does not
mention (see `Missing parts and issues`_). Configuration files are loaded only
when they are listed in the ``config`` setting, nothing from the config
directory is loaded automatically as configuration (see
:doc:`configuration/index`). Other code reads that directory directly, for
example client files, keys and ``credentials.cfg``.

Scheduling and state tracking
=============================

**Planned.** ``spinta sync`` can be run repeatedly (for example every minute by
cron), or as a long running process. Different targets must update at
different rates, so each run must know what was already synchronized and
when.

The Catalogue implements the UAPI Changes API (``/:changes``, see the UAPI
draft, ``openapi/docs.md``). Each model in the Catalogue has a changelog. Every
change in it has a change id (``_cid``) that grows with each change of that
model, and the revision of the object it changed. An agent asks the Catalogue
for the changes after the change id it has stored, applies them, and stores the
new change id. ``spinta keymap sync`` already works this way, see
`Keymap synchronization`_.

Each target has its own state file and its own lock, so that no two targets
write the same file, and targets that do not need each other can run at the same
time. The state of each Catalogue is kept in a directory:

.. code-block:: text

    $XDG_DATA_HOME/spinta/sync/<catalog>/
        manifests.json      state of the manifests target
        manifests.lock      lock of the manifests target
        ...                 the same pair for every other target

``<catalog>`` is the name of the Catalogue that the metadata is synchronized
from. It is the name of the Catalogue section in ``credentials.cfg``, for
example ``katalogas``. If only the old ``default`` section exists, the name is
``default``. The file names are the target names, so they are predefined like
the other sync-owned locations.

**What is stored.** One file holds the state of one target. It holds no
credentials and no synchronized values. The values are in the sync-owned files
of the target (see `Sync-owned files and locations`_). The file keeps:

* ``target`` — the name of the target. It must match the file name.
* ``last_run_at`` — the time of the last run of the target, successful or not.
* ``last_success_at`` — the time of the last successful run, or ``null``.
* ``status`` — ``changed``, ``unchanged`` or ``failed``, the same words that the
  run summary prints. ``changed`` means that the run applied at least one
  change.
* ``error`` — the error of the last run, or ``null``. Only the last error is kept.
* ``cursors`` — for targets that read the Catalogue changes, the last change id
  applied, per Catalogue model. A target that reads two models keeps two
  cursors, because each model has its own changelog. Two targets that read the
  same model keep separate cursors, so each can fail or be due independently.

Targets that send data to the Catalogue have no ``cursors``. They keep the
position of what they sent instead, see the last column of the table.

.. list-table::
   :header-rows: 1
   :widths: 14 30 56

   * - Target
     - Catalogue models read
     - Other fields
   * - :doc:`sync/manifests`
     - ``Dataset``
     - None
   * - :doc:`sync/backends`
     - Not decided, see :doc:`sync/backends`
     - None
   * - :doc:`sync/datasets`
     - ``Dataset``
     - None
   * - :doc:`sync/models`
     - ``Model``
     - None
   * - :doc:`sync/contracts`
     - ``Contract``
     - None
   * - :doc:`sync/clients`
     - ``Client``
     - None
   * - :doc:`sync/logs`
     - None (sends data)
     - ``last_sent_at``, the end of the last period sent to the Catalogue.
   * - :doc:`sync/push`
     - None (sends data)
     - ``sent_push_at``, the time of the push state sent to the Catalogue.

Revisions are not stored. Each change record carries the revision of the object
it changed. The agent needs revisions only when it writes back to the Catalogue
(Part 3, not planned yet), so the state does not keep them.

Example. ``manifests.json`` has a successful run. ``models.json`` failed on its
last run. Its cursor is still ``5209``, the last change id it applied, so the
next run asks again from ``5210``.

.. code-block:: json

    {
      "format_version": 1,
      "catalog": "katalogas",
      "agent": "example",
      "target": "manifests",
      "last_run_at": "2025-06-02T10:15:00Z",
      "last_success_at": "2025-06-02T10:15:00Z",
      "status": "changed",
      "error": null,
      "cursors": {"Dataset": 1842}
    }

.. code-block:: json

    {
      "format_version": 1,
      "catalog": "katalogas",
      "agent": "example",
      "target": "models",
      "last_run_at": "2025-06-02T10:15:02Z",
      "last_success_at": "2025-06-01T10:15:02Z",
      "status": "failed",
      "error": "GET /Model/:changes/5210 returned HTTP 503",
      "cursors": {"Model": 5209}
    }

**Due rule.** A target is due when its file is missing, when ``last_success_at``
is ``null``, or when the time since ``last_success_at`` is at least the interval
of the target. The interval is set per target in configuration. Intervals are
not stored in the file, because they are user settings. If no target is due, the
run does nothing and reports that everything is in sync. Daemon mode (a process
that keeps running and synchronizes by itself) is tracked in Katalogas #1584.

**Locking.** Each target has its own lock file, ``<target>.lock``. A run takes
the locks of the targets it runs before it reads any state:

* ``spinta sync <target>`` and ``spinta sync <target> <element>`` take the lock
  of that target.
* ``spinta sync`` without a target takes the locks of all targets, in the fixed
  order of the targets. It runs the targets one after another, so it holds all
  of the locks until the last target is done. It takes all locks even if only
  some targets are due, because it reads the state of every target.
* A daemon takes the locks of the targets of each of its runs.

Two runs never overlap on the same target. Runs for different targets can run
at the same time.

The lock is not waited on. If a lock is held by another run, the new run releases
the locks it has already taken, stops with a message that names the lock file,
changes no state and exits with a non-zero status. Waiting would let runs pile up
behind a cron job that runs every minute.

The lock is an operating system advisory lock (``flock`` on Linux and macOS).
The kernel releases it when the process ends, even after a crash, so a crashed
run never leaves a stale lock behind. The content of the lock file is not used.
Advisory locks may not hold on network file systems, so the state directory must
be on a local file system.

**Run steps.** For a due target, the agent:

1. Takes the lock of the target (see **Locking** above).
2. Reads the state file of the target. If the file is missing, or the target has
   no cursor yet, it does a full sync (see below).
3. For each model in ``cursors``, requests the changes after the stored change
   id, in pages: ``GET /{model}/:changes/{cursor + 1}?limit(n)``. The request
   starts at ``cursor + 1`` because the Catalogue answers from the given change
   id on, the same convention as ``spinta keymap sync``.
4. Applies each page to the sync-owned files of the elements that the changes
   name, and then stores the change id of the last change in that page as the
   new cursor.
5. Sets ``last_success_at``, ``status`` and clears ``error``.

**Full sync.** The first run has no cursor. The agent first reads the latest
change id (``GET /{model}/:changes/-1``), then reads all objects of the model,
then stores that change id as the cursor. Changes made during the full read are
then picked up by the next run.

**Unknown cursor.** UAPI says that when the change id does not match the one the
client has, the client must do a full sync, because the data was migrated or
altered in another way. The agent then discards the cursor and does a full
sync. How the Catalogue reports an unknown change id is not decided, see
`Missing parts and issues`_.

**Element runs.** ``spinta sync <target> <element>`` reads the changes of that
element only and does not move the target's cursors, because a cursor covers all
elements of the target. The next full run reads the same changes again.

**Writing rules.**

* Only ``spinta sync`` writes these files, and only while it holds the lock of
  the target that owns the file. Nobody edits them by hand.
* Each file is written to a temporary file next to it and then renamed, so a
  crash cannot leave a half-written file.
* A cursor moves only over changes that were applied. A run that stops half-way
  keeps the cursor of the last applied page, so the next run continues from
  there.
* A failed run changes only ``last_run_at``, ``status`` and ``error``.
  ``last_success_at`` keeps its value. Cursors are never moved back.
* Timestamps are UTC, in ISO 8601 form.
* ``format_version`` and ``target`` are checked before the file is used. If
  either does not match, the run stops and does not overwrite the file. A file
  that cannot be read stops only its own target.
* A missing file means that the target has not run yet, so it does a full sync.
  Deleting one file forces a full sync of that target only, and the other
  targets keep their cursors. For targets that send data (``logs`` and ``push``)
  this can send the same data again, so their files must not be deleted
  casually.

No state file exists today. ``spinta sync`` keeps nothing between runs. The
state files, the lock and the due rule have no GitHub issue yet. The closest is
Katalogas #1584 (daemon mode), which needs state between runs. The Catalogue
does not yet implement the Changes API for these models.

Synchronization parts
=====================

The high-level design of the synchronization parts is defined in
katalogas#2302 (matrix of cases for catalogue, source and agent). The three
parts (catalogue to agent, source to agent, agent to catalogue) all work on the
manifest, so they are described on the manifests target page, see
:doc:`sync/manifests`.

Current implementation
**********************

Only one target is implemented, the manifests target (Part 1, Catalogue to
Agent). It is implemented in ``spinta/cli/sync.py`` and in the helpers under
``spinta/cli/helpers/sync/``. In short:

* ``spinta sync`` takes no arguments. There is no ``<target>`` or
  ``<element>`` yet.
* The local manifest location comes from the user setting
  ``manifests.default.path``. It is not a predefined sync-owned location.
* The merged manifest is written back to that path and the whole manifest is
  printed to stdout.
* Credentials are read from ``credentials.cfg`` and the Catalogue is reached
  through its API.

The detailed steps, the merge rules and the helper code are described on
:doc:`sync/manifests`.

Missing parts and issues
************************

* **Targets other than manifests** — none of the other targets is implemented.
  See the target pages (:doc:`sync/backends`, :doc:`sync/datasets`,
  :doc:`sync/models`, :doc:`sync/contracts`, :doc:`sync/clients`,
  :doc:`sync/logs`, :doc:`sync/push`).
* **Element syntax** — ``spinta sync <target> <element>`` is not implemented.
  Each target page defines its element.
* **Predefined locations for sync-owned files** — not implemented. Today the
  manifest location is a user setting (``manifests.default.path``). No issue
  yet.
* **Priority of sync-owned files and per-element overrides** — the rule that
  sync-owned files have the lowest priority, and that a user can override a
  single value of a single element, are not implemented. No issue yet.
* **Settings** ``data_path`` and ``config_path``. The target design
  changes only ``XDG_DATA_HOME`` and ``XDG_CONFIG_HOME``. Whether the
  ``data_path`` and ``config_path`` settings remain supported for sync-owned
  files is not decided. No issue yet.
* **Catalogue Changes API** — the Catalogue must implement the UAPI Changes
  endpoint (``/{model}/:changes/{cid}`` and ``:changes/-1``) for the models in
  the table. Not implemented yet. No issue yet.
* **Unknown cursor signal** — how the Catalogue reports a change id it no longer
  knows, so that the agent can start a full sync. No issue yet.
* **Models read by each target** — the Catalogue model names (``Dataset``,
  ``Model``, ``Contract``, ``Client``) are not checked against the Catalogue
  yet. No issue yet.
* **Dataset changes and DSA** — whether a change to a dataset's models or
  properties also produces a change of the ``Dataset`` model. See
  the open questions of :doc:`sync/manifests`. No issue yet.
* **Per-element intervals** — the datasets page allows a dataset to have its own
  interval, but the state is per target. Decide whether the interval moves to
  configuration per element, or is dropped. No issue yet.
* **State files and lock** — nothing of ``$XDG_DATA_HOME/spinta/sync/<catalog>/``
  is implemented. Open points: support for ``flock`` on Windows, and the directory
  name when only the old ``default`` section exists. No issue yet. See
  `Scheduling and state tracking`_.
* **Sync-owned files across Catalogues** — sync-owned locations such as
  ``manifests/<dataset>.csv`` are not namespaced by Catalogue. Two Catalogues
  with the same dataset code name would write the same file, and the lock of one
  Catalogue does not prevent it. No issue yet.
* **Summary output** — ``spinta sync`` without a subcommand still prints the
  whole manifest. No issue yet.
* **Daemon mode** — katalogas#1584.
* **Part 2, source to agent** — spinta#1489.
* **Part 3, agent to catalogue** — katalogas#1942, katalogas#1600.
* **Versioning (several MAJOR versions per dataset)** — spinta#1647.
* **Multiple datasets per run** — spinta#1404 is closed; the loop over dataset
  ids already exists in the code, the remaining work is in Parts 2 and 3.
* **Output and dataset parameters** — spinta#2086 (see :doc:`sync/datasets`).
* **Pushing ``spinta push`` state to the Catalogue** — spinta#1416.

Synchronization targets
***********************

.. toctree::
   :maxdepth: 1

   sync/manifests
   sync/backends
   sync/datasets
   sync/models
   sync/contracts
   sync/clients
   sync/logs
   sync/push

Keymap synchronization
**********************

``spinta keymap sync`` is a different command and it is not part of
``spinta sync``. It synchronizes external identifiers between Spinta instances.
Keymaps map external primary keys to internal identifiers (`_id`, UUIDs), so
that when several Spinta instances publish the same data, each row keeps the
same internal ID.

The keymap sync command:

* fetches the changelog ``{server}/{model}/:changes`` from a remote Spinta server;
* converts change events into `KeymapSyncData` entries (insert, update, move mappings);
* writes the mappings to the keymap database;
* stores the last synchronized change id, so that the next run does not repeat work.

See :doc:`keymap/index` for keymap implementations and operations.

History
*******

Earlier designs planned to use ``spinta sync`` for schema migrations too. This
was dropped. ``spinta migrate`` handles schema migrations, and the ``spinta
sync`` name was reused for metadata synchronization with the Catalogue
(spinta#1310 tracked the first implementation, spinta#1496 is the parent task
of the synchronization parts).

The command is implemented by ``sync()`` in `spinta/cli/sync.py`. The older
``sync()`` handler in `spinta/cli/migrate.py` (which merged source manifests
with ``commands.sync(context, manifest)``) is no longer reachable.
`spinta/cli/main.py` registers ``sync`` twice, and the later registration of
``spinta.cli.sync.sync`` replaces the earlier one. The ``commands.sync``
overloads for manifests (`spinta/manifests/backend/commands/sync.py`,
`spinta/manifests/yaml/commands/sync.py`) were only called from that handler.
Keymap sync still uses ``commands.sync``.

.. seealso::

    * :doc:`sync/datasets` — dataset-level parameters and output design
    * :doc:`sync/manifests` — the implemented target, its merge rules and steps
    * :doc:`keymap/index` — keymap synchronization and identifier mapping
    * :doc:`data-access/changes` — the ``:changes`` endpoint used by keymap sync
    * :doc:`data-access/backends/internal-data-store` — ``spinta bootstrap`` and ``spinta migrate``
    * :doc:`user-interfaces/cli` — CLI commands and usage
    * :doc:`manifests/index` — manifest format and structure
    * :doc:`configuration/index` — how configuration files and directories are read
    * katalogas#2302 — high-level synchronization architecture: https://github.com/atviriduomenys/katalogas/issues/2302
