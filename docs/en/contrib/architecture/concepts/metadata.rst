.. default-role:: literal

********
MetaData
********

`spinta/components.py:MetaData` and
`spinta/components.py:ExtraMetaData` are `Node` subclasses (see
:doc:`node`) that split manifest entries into two groups.

`MetaData` — a first-class, named manifest entry: an entry that is
addressable by type and name and stored in one of the manifest object
lists. Subclasses: `Namespace`, `Model` (in `spinta/components.py`) and
`Dataset` (in `spinta/datasets/components.py`).

`ExtraMetaData` — a secondary entry that is attached to its parent node
and not stored in the top level manifest object lists. Subclasses:
`Property` and `Base` (both in `spinta/components.py`).

Attributes and schema
*********************

`EntryId = Union[int, str, pathlib.Path]` — a type alias of the metadata
entry ID; what it actually is depends on the manifest type: a file path
for YAML manifests, a UUID for backend manifests, a row number for CSV
tables.

`MetaData` adds:

- `eid` — the `EntryId`.
- `id` — the identifier given in the manifest (schemas may allow it to
  be missing, e.g. inline manifests).
- `schema` — declares the shared metadata attributes: `type` and `name`
  (required), `id`, `version`, `title`, `description`, `lang`.
- `MetaData.get_eid_for_error_context()` — renders `eid` as a path
  relative to the manifest path (when possible), for error messages.

`ExtraMetaData` only adds the `id` attribute.

Common subclasses in more detail
********************************

- `Namespace(MetaData)` — namespace tree node; holds `names` (child
  namespaces) and `models` dicts, `title`, `description`, `access`,
  optional `keymap` and `backend`. Namespace nodes are generated
  automatically from model names (`generated=True`) or declared
  explicitly.
- `Model(MetaData)` — a data model: `properties` dict of `Property`,
  `external` `Entity` (external source binding), `ns`, `mode`
  (`internal`/`external`), `access`, `level`, `base`, quick-access
  `id_prop`/`revision_prop`/`uri_prop` and pagination helper `page`.
- `Base(ExtraMetaData)` — base-class entry of an inherited model: links
  the child `model` with its `parent` model and copy of `pk` primary key
  properties.
- `Property(ExtraMetaData)` — one property: dotted path `place`,
  `dtype` (a `DataType` instance which does the actual work), `external`
  `Attribute` (the external source column), `prepare` formulas, `enum`,
  `unit`, `access`, `level` etc.

Top level object lists
**********************

The manifest keeps the loaded `MetaData` entries grouped by type in
`spinta/manifests/components.py:MetaDataContainer`:

- `ns` — namespaces (`Namespace`).
- `dataset` — datasets (`Dataset`).
- `model` — models (`Model`).

`ExtraMetaData` entries (`Property`, `Base`) are reachable only from
their parent nodes, which is why they don't need a name in these lists.

External specifications
***********************

- The `metadata` attributes (`title`, `description`, `lang`, `uri`) map
  to the RDF(S) descriptive vocabulary (https://www.w3.org/TR/rdf-schema/,
  local extract `.tmp/sources/w3c/RDF-Schema.md`): `rdfs:label`,
  `rdfs:comment`, and to DCAT
  (https://www.w3.org/TR/vocab-dcat-3/, local extract
  `.tmp/sources/w3c/vocab-dcat-3.md`): a model in a namespace corresponds
  to a `dcat:Dataset` in a `dcat:Catalog`, with `dcat:title`,
  `dcat:description`, `dcat:identifier`.
- The metadata entry ID (`eid`, `id`) and in reserved properties
  (`_id`, `_revision`) map to UAPI reserved properties
  (https://github.com/atviriduomenys/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`): every returned resource carries `_id`
  (the row ID) and `_revision`.
- DSA (https://dsa.readthedocs.io/, local extract `.tmp/sources/dsa/
  DSA.md`) defines the `dataset`/`resource`/`base`/`model`/`property`
  dimensions, which load into `Dataset`, (`Resource`), `Base`, `Model`
  and `Property` components.

See also
********

- :doc:`node` — the node base class.
- :doc:`/contrib/architecture/configuration/store` — where the loaded manifest trees are kept.
- :doc:`/reference/components` — autodoc API of some components
  (`Store`, `Config`, `RawConfig`).
