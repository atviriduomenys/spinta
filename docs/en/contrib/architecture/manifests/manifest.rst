:orphan:

.. default-role:: literal

********
Manifest
********

`spinta/manifests/components.py:Manifest` is the base class of all
manifest formats. A manifest instance is an in-memory container of loaded
metadata nodes plus format-specific load parameters (file path, DSN,
inline data, ...). Concrete formats — `csv`, `yaml`, `sql`, `rdf`, ... —
are `Manifest` subclasses registered in
`spinta/config.py:CONFIG['components']['manifests']` (see
:doc:`/contrib/architecture/manifests/index` for the full list).

Attributes
**********

- `type` — manifest type string, e.g. `"csv"`, `"yaml"`, `"sql"`. The
  class is looked up by this string in `components.manifests`.
- `name` — manifest name from the `manifests` config section (e.g.
  `"default"`).
- `keymap` — the `KeyMap` instance assigned from the `keymaps` config
  section (default `"default"`), see :doc:`/contrib/architecture/configuration/store`.
- `backend` — the internal `Backend` assigned from the `backends` config
  section (default `"default"`).
- `backends` — backends declared *in the manifest itself* (e.g. by
  `resource` rows without a dataset in tabular manifests), loaded by
  `spinta/manifests/helpers.py:_load_manifest_backends()`.
- `parent` — the parent component (`None` for the main manifest).
- `store` — back reference to the `Store` (see :doc:`/contrib/architecture/configuration/store`).
- `_objects` — the loaded node tree, a `MetaDataContainer` typed dict
  with `ns`, `dataset` and `model` object lists; accessed via
  `Manifest.get_objects()`.
- `path` — format specific location: file path for `csv`, `ascii`,
  `xlsx`, `yaml`, `json`, `xml`, `rdf`, `xsd` manifests; a URL for
  `gsheets`; a DSN for `sql` and `internal` manifests; `None` for
  `inline` and `memory`.
- `access` — default `Access` level (`spinta/core/enums.py:Access`) for
  the whole manifest, from the `manifests.<name>.access` option
  (`config.default_access_level` by default).
- `prefixes` — loaded `UriPrefix` entries
  (`spinta/dimensions/prefix/components.py`).
- `enums` — loaded `Enums` (`spinta/dimensions/enum/components.py`).
- `sync` — list of other `Manifest` instances this manifest loads its
  nodes from (the `manifests.<name>.sync` config option; cycle detection
  in `_configure_manifest()`).
- `mode` — `Mode.internal` or `Mode.external`
  (`spinta/core/enums.py:Mode`), from `manifests.<name>.mode`.
- `given` — a `ManifestGiven` object holding the given (as written by
  the user) values, currently just `given.access`.
- `dynamic` — `True` for manifests that can be updated at runtime
  (currently `InternalSQLManifest`, `spinta/manifests/internal_sql/
  components.py`).

`detect_from_path()` is a static method used to guess the manifest type
from a path or URL — see :doc:`manifestpath` and :doc:`/contrib/architecture/manifests/index`.

For tests, `Manifest.__eq__()` accepts a string and compares it with the
manifest rendered as an ASCII table via
`spinta/testing/manifest.py:compare_manifest()` (hooked through
`pytest_assertrepr_compare`), which makes assertions like
`assert manifest == 'd | r | m | ...'` work.

Creation and loading
********************

Manifest instances are created in `spinta/manifests/helpers.py`:

- `create_manifest(context, store, name)` — instantiates the class
  registered under `manifests.<name>.type`, calls `init_manifest()`
  (empty `_objects` per node type, `prefixes`, `enums`, `access`,
  `mode`, empty `sync`) and `_configure_manifest()` (access, keymap,
  backend, mode, sync), then dispatches `commands.configure`.
- `create_internal_manifest(context, store)` — creates the
  `InternalManifest` (`spinta/manifests/internal/components.py`, a
  `YamlManifest` subclass) whose `configure` command points `path` to
  the packaged `spinta/manifest/` directory with the internal models
  (`_ns.yml`, `_schema.yml`, `_txn.yml`).
- `clone_manifest(context, name)` — creates an empty manifest, reusing
  the keymap of `store.manifest` (used e.g. to build manifests on the
  fly, like `spinta inspect`).

Nodes are read by the per-type `load` command overloads (e.g.
`spinta/manifests/tabular/commands/load.py`) which produce
`(eid, schema)` tuples and pass them to
`spinta/manifests/helpers.py:load_manifest_nodes()`; that function
instantiates nodes via `spinta/nodes.py:get_node()` and stores them into
the `_objects` lists (see :doc:`/contrib/architecture/concepts/node` and :doc:`/contrib/architecture/concepts/metadata`).

External specifications
***********************

- DSA (https://dsa.readthedocs.io/, local extract `.tmp/sources/dsa/
  DSA.md`) — the tabular manifest (`csv` type) is Spinta's
  implementation of the DSA data structure description table: a manifest
  corresponds to a whole DSA table, with the loaded `Namespace`,
  `Dataset` and `Model` nodes corresponding to the DSA dimensions.
- RML (https://rml.io/specs/rml/, local extract `.tmp/sources/rml/
  RML.md`) — a manifest plays the role of an RML *mapping*: declarative
  rules describing how records of an external source (RML's *logical
  source*) are turned into a domain model. In Spinta the mapping rules
  are the manifest rows themselves (`source`, `prepare` columns), and
  the target is the internal model tree instead of RDF triples.
- DCAT (https://www.w3.org/TR/vocab-dcat-3/, local extract
  `.tmp/sources/w3c/vocab-dcat-3.md`) — a manifest resembles a
  `dcat:Catalog` record: it carries dataset and service descriptions
  (titles, access rights, distributions) from which the API is built.

See also
********

- :doc:`/contrib/architecture/manifests/index` — manifests overview and how they are loaded.
- :doc:`manifestpath` — `ManifestPath`, the parsed manifest location.
- :doc:`tabular-manifest` — the tabular manifest classes.
- :doc:`/contrib/architecture/concepts/metadata` — what the loaded `_objects` node lists contain.
- :doc:`/contrib/architecture/manifests/logical/datasets` — the `Dataset` entries held in `_objects.dataset`.
- :doc:`/contrib/architecture/configuration/store` — where manifests are kept at runtime.
