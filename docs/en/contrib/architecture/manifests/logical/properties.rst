.. default-role:: literal

********
Property
********

`spinta/components.py:Property` is an `ExtraMetaData` subclass (see
:doc:`/contrib/architecture/concepts/metadata`) that describes one property of a `Model` (see
:doc:`models`). Unlike `MetaData` entries, properties are not stored in
the manifest's top-level object lists — they are attached to their
parent model (`Property.model`) via `Model.properties`.

Attributes
**********

- `name` — property name (e.g. `"code"`, or a reserved name like
  `"_id"`); `place` — the dotted path inside the model (for nested
  properties of `object`/`ref`/`array` types, e.g.
  `"address.city"`); `basename` (from :doc:`/contrib/architecture/concepts/node`) is the last fragment.
- `model` — the parent `Model`.
- `dtype` — the `DataType` instance that does the actual work: type
  name, unique/nullable/required flags, `prepare` formula and
  backend-specific behavior (see `spinta/types/datatype.py`). The same
  property class is used for all types — the type behavior lives in
  `dtype`, not in `Property` subclasses.
- `external` — the `Attribute` (from `spinta/datasets/components.py`)
  binding the property to the external source column.
- `given` (`PropertyGiven`) — values explicitly given in the manifest:
  `access`, `enum` (enum name), `unit`, `explicit` (whether the
  property was explicitly declared or generated), `prepare` formulas.
- `access` and `level` — `Access` and `Level` maturity enums, inherited
  from the model by default (`model.access`, `model.level`).
- `enum` / `enums` — enum values of this property, from the `enum`
  dimension (see `spinta/dimensions/enum/`).
- `title`, `description`, `link`, `uri`, `lang`, `comments` —
  descriptive metadata; `hidden` — property is not shown in schema
  listings; `status`, `visibility`, `count`, `origin` — lifecycle
  metadata like on `Model`.
- `list` — parent property for properties inside arrays.

Reserved properties
*******************

Properties whose names start with `_` are reserved. `Property.is_reserved()`
tells if a property is reserved. Reserved properties are added
automatically to every model in
`spinta/nodes.py:load_model_properties()`:

- `_op`, `_type`, `_id` (type `pk`, unique), `_revision`, `_txn`,
  `_cid`, `_created`, `_where` (type `rql`), `_base` (type `inherit`),
  `_page` (type `page`), `_uri`, `_same_as`.

`_id` and `_revision` are exposed as model quick-access properties
(`Model.id_prop`, `Model.revision_prop`). Name validation
(`spinta/types/helpers.py:check_property_name`) distinguishes
generated reserved properties (`RESERVED_PROPERTY_NAMES`) from the
ones a data steward may explicitly give (`ALLOWED_RESERVED_PROPERTY_NAMES`,
e.g. `_id` with an external source or model `unique` constraint).

Property lifecycle
******************

- load — properties are created from the model's manifest schema by
  `spinta/nodes.py:load_model_properties()`, each is loaded with the
  `load(Context, Property, dict, Manifest)` command; the type string in
  the schema selects the `DataType` subclass which is then loaded into
  `prop.dtype` (e.g. `spinta/types/datatype.py:load(Context, UUID, ...)`
  for `pk`). `DataType.load()` registers leaf properties into
  `model.leafprops` via `_add_leaf_props()`. `configure(Context,
  Property)` applies per-property config from `rc` under
  `models.<name>.properties.<place>`.
- link — `spinta/types/model.py:link(Context, Property)`: links
  `dtype`, the external `Attribute`, resolves access inheritance from
  the model/namespace chain (private properties are only selectable
  with an explicit property scope, see `property_is_private()`) and
  links enum values.
- check — `spinta/types/model.py:check(Context, Property)`: checks the
  property name and enum definitions, then checks `dtype`.
- authorize — `authorize(Context, Action, Property)` is used when a
  property is accessed directly (e.g. property content of references).

`Property.model_type()` returns the full unique path
(`model.name + "." + place`), used e.g. for error contexts and keymap
scopes.

External specifications
***********************

- UAPI (https://github.com/atviriduomenys/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`): properties are selected with `_select`,
  sorted with `_sort` and appear as JSON object members. Reserved
  properties map to UAPI reserved members `_type`, `_id`, `_revision`,
  `_page` (see :doc:`/contrib/architecture/data-access/pagination`).
- RDFS (https://www.w3.org/TR/rdf-schema/, local extract
  `.tmp/sources/w3c/RDF-Schema.md`): a property maps to `rdf:Property`
  with `rdfs:domain` (the model) and `rdfs:range` (the property's
  datatype, i.e. `prop.dtype`); `title`/`description` map to
  `rdfs:label`/`rdfs:comment`.
- Conceptual model (https://raw.githubusercontent.com/atviriduomenys/
  katalogas/f8ad4b7d9f332ed9f9ada6c4e3e735512745e328/uml.drawio.svg, local
  extract `.tmp/sources/atviriduomenys/conceptual-model.md`): the ubiquitous
  language of the Lithuanian open data catalogue uses the same term
  `Property` — a one-to-one name mapping; there `Property` is a
  `sh:PropertyShape` that belongs to `Model` (1..1), points to `DataType`
  (`type`) and optionally to the source-side `SourceProperty` (`source`,
  see :doc:`/contrib/architecture/manifests/physical/attributes`).
- DCAT (https://www.w3.org/TR/vocab-dcat-3/, local extract
  `.tmp/sources/w3c/vocab-dcat-3.md`): property descriptive metadata
  maps to `dcat:title`, `dcat:description`, `dcat:identifier`.
- DSA (https://dsa.readthedocs.io/, local extract `.tmp/sources/dsa/
  DSA.md`): the `property` dimension with the type column, `enum`,
  `unit` and the formula language that loads into `prepare` formulas
  (see :doc:`/contrib/architecture/multiple-dispatch/ufuncs/ufunc`).

See also
********

- :doc:`models` — the parent of every property.
- :doc:`/contrib/architecture/data-access/pagination` — the `_page` reserved property and pagination helpers.
- :doc:`/contrib/architecture/manifests/physical/attributes` — the `Attribute` external source binding stored in
  `Property.external` for properties of external models.
- :doc:`/contrib/architecture/concepts/metadata` — `ExtraMetaData` vs `MetaData`.
- `spinta/types/datatype.py` — property type implementations.
