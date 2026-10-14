.. default-role:: literal

######
Keymap
######

`spinta/datasets/keymaps/components.py:KeyMap` is a `Component` subclass
(see :doc:`/contrib/architecture/concepts/component`) that maps *external* primary keys — whatever the
data source uses, e.g. arbitrary strings or composite keys — to the
*internal* identifiers (`_id`, UUIDs) used by Spinta. It implements the
DSA "identifiable" maturity step: once data is identifiable (level 4,
`Level.identifiable` in `spinta/core/enums.py`), every row has a stable
internal id even when the source key changes or is composite.

A `KeyMap` is a separate database (not the internal data backend), one
per configured keymap name. It is used by:

- **datasets / models** — every external model resolves its keymap
  (`model.keymap`), see :doc:`/contrib/architecture/manifests/logical/models`;
- **manifests** — the ``keymap`` column of the manifest (a `ref` to the
  `keymaps` config section) selects the keymap per manifest, dataset or
  model row (see :doc:`/contrib/architecture/manifests/manifest` and :doc:`/contrib/architecture/manifests/tabular-reader`);
- **pull / sync** — `spinta pull` and `spinta keymap sync` write the
  external-to-internal mapping (see `Keymap sync`_ below).

Interface
*********

`KeyMap` defines the operations, all raising `NotImplementedError` on
the base class:

- `encode(name, value, primary_key=None)` — from external to internal:
  given a model name and the external key value, returns (and stores)
  the internal key. `None` values are not encoded (they return `None`).
- `decode(name, key)` — from internal to external: the inverse lookup,
  returning the original key value.
- `contains(name, value)` — whether the external key is already mapped.
- `has_synced_before()`, `get_last_synced_id(name)`,
  `update_sync_data(name, cid, time)` — sync bookkeeping (see below).
- `synchronize(data)` — store one `KeymapSyncData` row.
- `validate_data(name)` — check a model's keymap table for duplicate
  values.
- `copy()` — a copy with fresh connection state (used to attach a
  per-request instance to the context).

Implementations
***************

Two implementations are registered in `spinta/config.py` under
`components.keymaps`:

- ``sqlalchemy`` — `spinta/datasets/keymaps/sqlalchemy.py:
  SqlAlchemyKeyMap` — a SQL database (SQLite by default in the ``dev``
  environment: ``sqlite:///{data_dir}/keymap.db``; any SQLAlchemy DSN
  works). Each model gets a table named after the model (`model_type()`)
  with ``key``, ``value``, ``redirect`` and ``modified_at`` columns;
  values are stored as JSON (`prepare_value()`), new keys are random
  UUIDs. Also has a ``_synchronize`` table (one row per model with the
  last synced change id, see `Keymap sync`_) and a ``_migrations`` table
  validated at `prepare` time.
- ``redis`` — `spinta/datasets/keymaps/redis.py:RedisKeyMap` — Redis
  hashes per model: ``keymap:<model>:values`` (serialized value → key),
  ``keymap:<model>:keys`` (key → value) and ``keymap:<model>:meta`` plus
  a ``keymap:sync`` hash.

A keymap is configured in the ``keymaps`` rc section (``type`` and
``dsn``) and loaded into `store.keymaps` by `load(Context, Store)` (see
:doc:`/contrib/architecture/configuration/store`); its DSN is read by the ``configure`` overload
(`spinta/datasets/keymaps/sqlalchemy.py`) and the engine is created at
``prepare`` time.

How a model gets its keymap
***************************

- The manifest row declares ``keymap`` (a keymap *name*). At model load
  (`spinta/types/model.py`) `model.keymap` is resolved from
  `store.keymaps`, falling back to `manifest.keymap` (the manifest-level
  default) when the model does not specify one (see :doc:`/contrib/architecture/manifests/manifest`).
- At ``link`` time, an external model without a resolvable keymap raises
  `KeymapNotSet` (`spinta/types/model.py:link(Context, Model)`).
- For CLI operations keymap instances are attached to the context under
  ``keymap.<name>`` — as *copies* with fresh connections
  (`spinta/cli/helpers/store.py:attach_keymaps()` calling `KeyMap.copy()`),
  so a request can use ``context.get(f"keymap.{model.keymap.name}")``
  (e.g. the ``getone`` implementation in
  `spinta/datasets/backends/sql/commands/read.py` decodes the requested
  `_id` back to the source key with `keymap.decode()`).

Keymap property combinations
****************************

Spinta keeps not only the whole-key mapping, but also mappings for
*combinations* of properties that are used as reference keys. `Model`
collects them in `required_keymap_properties` (a list of property-place
tuples, `spinta/components.py:Model.add_keymap_property_combination()`):

- when a :doc:`/contrib/architecture/manifests/logical/base-models` copies its primary key into the inheriting model
  (`spinta/types/model.py`),
- when a :doc:`/contrib/architecture/manifests/logical/properties` of type `ref` declares which reference
  properties the lookup uses (`spinta/types/ref/link.py`).

Each combination gets its own keymap table named
``<model>.<prop1>_<prop2>``, filled during sync.

Reading and writing through the keymap
**************************************

- On **read** of external data, `_id` values are built with
  `keymap.encode()` in `spinta/datasets/commands/cast.py`
  (`process_data_for_pkey()` — see `spinta/datasets/helpers.py`), so
  external rows get stable internal identifiers; on `getone` the
  requested `_id` is decoded back to source key values.
- On **write** to the internal backend, already-keymap-encoded ids are
  passed through unchanged (identifiable models keep the synced `_id`).

Keymap sync
***********

The synchronization machinery in `spinta/datasets/keymaps/sync.py`
(powered by the ``spinta keymap sync`` CLI command, `spinta/cli/
keymap.py`) fills the keymap from a *remote* Spinta server without
pulling the data itself:

1. for each model it computes keymap names (the model and each
   `required_keymap_properties` combination) and the minimum
   not-yet-synced change id (`_cid_offset()` using
   `get_last_synced_id()`);
2. it fetches the remote changelog ``{server}/{model}/:changes`` page by
   page (`_fetch_changelog_data()`, `config.sync_page_size` rows);
3. it converts rows into `KeymapSyncData` entries (the dataclass in
   `spinta/datasets/keymaps/components.py`): inserts (new internal
   ``_id``), updates (remapped property values via `keymap.decode()`)
   and moves (``_same_as`` — the keymap row gets a ``redirect`` to the
   new id, `sync_model_move()`), yielding rows for each model keymap;
4. the ``commands.sync`` overload (`spinta/datasets/keymaps/
   sqlalchemy.py`, registered per keymap type) calls
   `keymap.synchronize()` row by row, batching transactions by
   `sync_transaction_size`;
5. after each model, the last synced cid is stored
   (`update_sync_data()`) and `validate_data()` reports duplicate
   values (`KeymapDuplicateMapping` unless ``duplicate_warn_only``).

`KeymapSyncData.identifiable` says whether the row belongs to a model
with a declared primary key (`is_model_identifiable()` — the model's
`external.unknown_primary_key` is `False`); legacy rows without a
primary key are still stored by the backwards-compatibility
`__legacy_synchronize()` path, which can create duplicate mappings and
is scheduled for removal.

External specifications
***********************

- UAPI (https://github.com/atviriduomenys/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`): the reserved ``_id`` member that clients
  see in the API is the keymap-encoded internal identifier; keymap
  ``decode`` is what lets ``/model/_id`` requests be translated back to
  source keys.
- DCAT (https://www.w3.org/TR/vocab-dcat-3/, local extract
  `.tmp/sources/w3c/vocab-dcat-3.md`): keymap-backed stable identifiers
  are what make data "identifiable" (`Level.identifiable`) — a
  precondition for DCAT `dcat:Distribution`-style linked publishing
  (`Level.linked`).
- RML (https://rml.io/specs/rml/, local extract `.tmp/sources/rml/
  RML.md`): the keymap plays the role of the join table RML achieves
  with ``rr:parentTriplesMap``/``rr:joinCondition`` — mapping source
  identifiers to target-object identifiers. See :doc:`/contrib/architecture/data-access/backends/external-backend`
  and :doc:`/contrib/architecture/manifests/physical/resources` for the RML parallel.

See also
********

- :doc:`/contrib/architecture/manifests/logical/models` — how `model.keymap` is resolved and when `KeymapNotSet`
  is raised.
- :doc:`/contrib/architecture/manifests/manifest` — the manifest-level ``keymap`` column.
- :doc:`/contrib/architecture/data-access/backends/external-data-sources` — keymap usage in the datasets
  layer.
- :doc:`/contrib/architecture/configuration/store` — `store.keymaps` and
  the loading order.

.. toctree::
   :maxdepth: 1

   Generated identifiers <generated-identifiers>
   Re-used identifiers <reused-identifiers>
