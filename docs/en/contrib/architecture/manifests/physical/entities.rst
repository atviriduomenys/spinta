.. default-role:: literal

******
Entity
******

`spinta/datasets/components.py:Entity` is an `External` (an `ExtraMetaData`
subclass, see :doc:`/contrib/architecture/concepts/metadata`) that binds one :doc:`../logical/models` to one external
source: which dataset and resource the rows come from, which properties form
the primary key in the source and what the source table or object is called.
It is created automatically when a model row in a manifest has external
columns and is stored in `model.external`.

Attributes
**********

- `model` — the parent `Model` (set before load in
  `spinta/types/model.py`).
- `dataset` — dataset name; after `link` — the `Dataset` instance (see
  :doc:`../logical/datasets`).
- `resource` — resource name; after `link` — the `Resource` instance (see
  :doc:`resources`). May be `None` when the model uses the dataset without a
  named resource.
- `pkeys` — list of `Property` instances (see :doc:`../logical/properties`) that form the
  source primary key, given in the ``pk`` column (`schema` entry `pk` with
  `attr: "pkeys"` and references into `model.properties`).
- `unknown_primary_key` — set when no ``pk`` was given; then `pkeys` falls
  back to all model properties (sorted by place).
- `name` — the source table or object name (the ``source`` column; read
  through `model.external.name` by external backends, e.g.
  `getall(Context, Model, Sql)` uses it as the table name). May contain
  ``{param}`` placeholders resolved from resource params.
- `prepare` — a spyna formula (see :doc:`/contrib/architecture/multiple-dispatch/ufuncs/expr`) merged with queries when
  reading this model's data.
- `params` — `Param` objects, same as on resources.

Entity lifecycle
****************

- created at model load — `spinta/types/model.py:load(Context, Model, dict,
  Manifest)`: the external columns of the model row are turned into a node
  with `get_node(..., group="datasets", ctype="entity")` and loaded with
  `load_node` + `commands.load`. The given ``pk`` names are kept in
  `model.given.pkeys`.
- load — `spinta/datasets/commands/load.py:load(Context, Entity, dict,
  Manifest)`: resolves `pkeys` from property names (or falls back to all
  properties with `unknown_primary_key = True`) and converts `prepare` to an
  `Expr`. Note the `XXX` comment in the source: `dataset` and `resource`
  start as strings and only become objects after `link` — a known wart
  (see issue 44 in the Spinta issue tracker).
- link — `link(Context, Entity)`: resolves `dataset` and `resource` strings
  into objects from the manifest, raises `MissingReference` if they do not
  exist, and registers the model in `entity.resource.models`.
- wipe — `wipe(Context, Entity, ExternalBackend)` is not implemented
  (`spinta/datasets/commands/wipe.py`).

External specifications
***********************

- RML (https://rml.io/specs/rml/, local extract `.tmp/sources/rml/RML.md`):
  an entity plays the role of an RML *triples map's* subject map — it says
  that rows of the logical source (the resource) become instances of a
  target class (the model) and which columns identify them (`pkeys`); the
  column-to-property term mappings are done by :doc:`attributes`.
- Conceptual model (https://raw.githubusercontent.com/atviriduomenys/
  katalogas/f8ad4b7d9f332ed9f9ada6c4e3e735512745e328/uml.drawio.svg, local
  extract `.tmp/sources/atviriduomenys/conceptual-model.md`): the ubiquitous
  language of the Lithuanian open data catalogue maps the entity to
  `SourceModel` — the model of data in the external source system (there
  `SourceModel` is an `rr:SubjectMap`, matching the RML reading above, and
  `SourceModel` reads from a `DataSource` which in turn points to a
  `Distribution` — the ubiquitous-language counterpart of the entity's
  `dataset`/`resource` bindings).
- DSA (https://dsa.readthedocs.io/, local extract `.tmp/sources/dsa/DSA.md`)
  defines the external columns of the `model` dimension (`dataset`,
  `resource`, `pk`, `source`, `prepare`) that load into `Entity`.
- UAPI (https://github.com/atviriduomenys/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`): entities are invisible to API clients —
  they only describe how `Model` instances (UAPI resources) are sourced.

See also
********

- :doc:`resources` — where the rows are read from.
- :doc:`attributes` — per-property column bindings.
- :doc:`../logical/models` — the model the entity binds to (`model.external`).
