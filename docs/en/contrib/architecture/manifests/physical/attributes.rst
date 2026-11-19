.. default-role:: literal

*********
Attribute
*********

`spinta/datasets/components.py:Attribute` is an `External` (an
`ExtraMetaData` subclass, see :doc:`/contrib/architecture/concepts/metadata`) that binds one
:doc:`../logical/properties` to one column of the external source. It is stored in
`Property.external` and only exists for properties of external models (see
:doc:`entities`).

Attributes
**********

- `prop` — the parent `Property` (set by `load_node` as `parent`).
- `name` — the source column name (the ``external`` column of the
  ``property`` row, or the property name itself when only a string was
  given). External backends select data by this name, e.g. the dask reader
  uses `prop.external.name` as the DataFrame column and the SQL backend
  reads `external.name` when building `_id` filters.
- `prepare` — a spyna formula (see :doc:`/contrib/architecture/multiple-dispatch/ufuncs/expr`) applied to the raw column
  value when casting it to the property type; `NA` when not given.
- `custom_type` — an optional type override (a class or instance) that
  replaces the default backend column type for this property. It can be
  set not only from the manifest, but also from the configuration under
  `models.<name>.properties.<place>.type` (resolved with `pydoc.locate` in
  `spinta/types/model.py`).

Attribute lifecycle
*******************

- created at model load — when a property row has external data,
  `spinta/types/model.py:_load_property_external()` creates a node with
  `get_node(..., group="datasets", ctype="attribute")`, loads it and stores
  it in `prop.external`. The per-property configuration above is applied
  before load, while the external data is still a plain dict.
- load — `spinta/datasets/commands/load.py:load(Context, Attribute, dict,
  Manifest)`: converts `prepare` to an `Expr` (or `NA`).
- link — `link(Context, Attribute)` is a no-op: the binding is consumed by
  backends during reads (`spinta/datasets/commands/cast.py` casts source
  values via `cast_backend_to_python`).
- PostgreSQL specifics: `spinta/backends/postgresql/helpers/type.py`
  validates that properties with `custom_type` don't clash with internal
  storage and uses `custom_type` as the column type when generating SQL.

External specifications
***********************

- RML (https://rml.io/specs/rml/, local extract `.tmp/sources/rml/RML.md`):
  an attribute corresponds to one *predicate-object map* term mapping of an
  entity's triples map: `attribute.name` is the source reference
  (`rml:reference`), `attribute.prepare` is a term transform, and
  `attribute.custom_type` plays the role of `rr:termType` overrides.
- DSA (https://dsa.readthedocs.io/, local extract `.tmp/sources/dsa/DSA.md`)
  defines the `external` (source column) and `prepare` metadata columns of
  the ``property`` dimension.
- SHACL (https://www.w3.org/TR/shacl/, local extract `.tmp/sources/w3c/
  SHACL.md`): there is no direct counterpart — SHACL constrains shapes of
  the target graph, while `Attribute` only describes the *source* side of a
  property; the target-side constraints are the property type and
  `DataType` flags.
- Conceptual model (https://raw.githubusercontent.com/atviriduomenys/
  katalogas/f8ad4b7d9f332ed9f9ada6c4e3e735512745e328/uml.drawio.svg, local
  extract `.tmp/sources/atviriduomenys/conceptual-model.md`): the ubiquitous
  language of the Lithuanian open data catalogue maps the attribute to
  `SourceProperty` — a property of the external source system (there
  `Property` has 0..* `SourceProperty` relations, and each `SourceProperty`
  belongs to a `SourceModel`, the ubiquitous-language counterpart of
  :doc:`entities`; like with RML above, this describes only the source side).

See also
********

- :doc:`../logical/properties` — the property being bound (`Property.external`).
- :doc:`entities` — the model-level binding that owns the source table.
- :doc:`/contrib/architecture/data-access/backends/external-data-sources` — how attributes are used when reading data.
