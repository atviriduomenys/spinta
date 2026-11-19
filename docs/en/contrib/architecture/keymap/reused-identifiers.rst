.. default-role:: literal

####################
Re-used identifiers
####################

Re-used identifiers are internal `id` values that Spinta keeps stable
across sources, datasets and re-synchronizations — when the same
external object shows up again (possibly in another dataset or with
a changed key), it gets the *same* internal identifier instead of
a new one. This is the lookup side of the keymap (`KeyMap.encode()`
returning an existing key, `KeyMap.decode()` mapping back, and the
`redirect` machinery for moved objects).

How identifiers are re-used
***************************

- `KeyMap.encode()` — before generating a new UUID, the keymap looks up
  the stored value (with no `redirect`) and returns the existing key;
  when a primary key was *given* (e.g. `pk` manifest column or
  `Base`-inherited keys) it is validated against the found key and
  re-used as is (`KeyMapGivenKeyMissmatch` on conflict).
- Composite and property-combination keys — every
  `required_keymap_properties` combination of a model gets its own
  keymap table (`model.add_keymap_property_combination()`), so ref
  lookups re-use identifiers per key combination (see
  :doc:`/contrib/architecture/keymap/index`).
- Keymap synchronization — `spinta keymap sync`
  (`spinta/datasets/keymaps/sync.py`) fills the keymap from a remote
  server's changelog: inserted and updated rows re-map rows by decoded
  identifier values (`sync_model_update()` decoding the old key and
  producing the new mapping), and moved rows (``_same_as``, the MOVE
  action) are recorded as `redirect`s — the keymap row of the old id
  points to the new id (`sync_model_move()`,
  `KeymapSyncData.redirect`).
- Redirects on reads — `getone` follows the redirect: the PostgreSQL
  `redirect` table (`commands.redirect`, `spinta/backends/postgresql/
  commands/redirect.py`) resolves an old `_id` to the new one and the
  API answers with a 301 redirect (`spinta/commands/read.py`).
- Redirects on writes — the MOVE action inserts into the redirect table
  (`commands.create_redirect_entry`) and records ``_same_as`` in the
  changelog (see `spinta/commands/write.py:move()`).

External specifications
***********************

- UAPI (https://github.com/atviriduomenys/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`): "There can't be two different global
  identifiers, pointing to the same building in any dataset" and
  "Different datasets and different models might have multiple local
  identifiers ... all local identifiers must be mapped to a single
  global identifier". The same example dataset/model pair must resolve
  to one `_id` — this is exactly what re-using the stable internal id
  across datasets and models guarantees.
- DSA (https://ivpk.github.io/dsa/, local extract
  `.tmp/sources/dsa/DSA.md`): the `base` dimension links models that
  describe the same real-world entity (see :doc:`/contrib/architecture/manifests/logical/base-models`);
  `_id`/`_revision` must remain stable so revisions and references keep
  pointing to the same object.
- RML (https://rml.io/specs/rml/, local extract `.tmp/sources/rml/
  RML.md`): the keymap plays the role of the join table RML achieves
  with `rr:parentTriplesMap`/`rr:joinCondition` — mapping source
  identifiers to target-object identifiers, re-used across mappings.

See also
********

- :doc:`/contrib/architecture/keymap/index` — the keymap component,
  the `decode()` path and keymap synchronization.
- :doc:`generated-identifiers` — when new identifiers are generated.
- :doc:`/contrib/architecture/manifests/logical/base-models` — base
  models linking models describing the same entity.
