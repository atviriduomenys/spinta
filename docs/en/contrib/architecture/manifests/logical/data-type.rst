.. default-role:: literal

********
DataType
********

`spinta/types/datatype.py:DataType` is the base class of all *property
types*. Every `Property` (see :doc:`properties`) carries one instance in
`Property.dtype`; while `Property` is manifest metadata, the type
instance does the per-type work — value casting, validation, backend
storage commands and query handling.

`DataType` extends `Component` (see :doc:`/contrib/architecture/concepts/component`) with the schema::

   schema = {
       "type": {"type": "string", "attr": "name"},
       "type_args": {"type": "array"},
       "unique": {"type": "bool", "default": False},
       "nullable": {"type": "bool", "default": False},
       "required": {"type": "bool", "default": False},
       "default": {"default": None},
       "prepare": {"type": "spyna", "default": None},
       "choices": {},
   }

Attributes
**********

- `type` — during load it is set to `"type"` (the node type, see
  :doc:`/contrib/architecture/concepts/node`), while `name` keeps the declared type string (e.g.
  `"string"`, `"pk"`); `type_args` — extra type arguments, e.g. in
  `"string(100)"`.
- `unique`, `nullable`, `required`, `default` — value constraints given
  in the manifest.
- `prepare` — the spyna formula applied to values (see :doc:`/contrib/architecture/multiple-dispatch/ufuncs/expr`,
  :doc:`/contrib/architecture/multiple-dispatch/ufuncs/ufunc`).
- `choices` — allowed values (an enum reference is resolved at link
  time, see `spinta/dimensions/enum/`).
- `prop` — back reference to the `Property`; the type instance is
  always property-bound (`repr` is `<{prop.name}:{name}>`).
- `backend` — the backend this property is stored in (resolved from the
  model backend, see :doc:`models`); per-backend behavior is implemented
  in command overloads, not on the class.
- `expandable` — the type participates in `expand` (e.g. `ref` types
  pull referenced models into the result).
- `requires_source`, `inherited` — external-source requirements and
  inheritance flags (e.g. properties inherited via `inherit` from the
  base model).

Type registry
*************

Type classes are registered in
`spinta/config.py:CONFIG['components']['types']` as `type name → class`
import strings. A property's type is instantiated in
`spinta/types/model.py` with `get_node(group="types")` (see
:doc:`/contrib/architecture/concepts/node`) and stored in `prop.dtype`. Names used in tabular manifests
must be from `DataTypeEnum` (`spinta/manifests/tabular/constants.py`;
e.g. `string`, `text`, `ref`, `backref`, `array`, `generic`,
`inherit`, `unknown`), and object/array property trees are parsed by
`PropertyReader` (see :doc:`/contrib/architecture/manifests/tabular-reader`).

Registered types (excerpt):

* `pk` (`PrimaryKey`) — the reserved `_id` property type.
* `string` (`String`), with `url` and `uri` subclasses; `text` (`Text`,
  `spinta/types/text/`), `binary` (`Binary`), `base32` (`Base32`).
* `integer` (`Integer`), `number` (`Number`), `boolean` (`Boolean`),
  `uuid` (`UUID`), `unknown` (`Unknown`).
* `date`, `time`, `datetime` (`Date`, `Time`, `DateTime`), with the
  `temporal` alias mapping to `DateTime`.
* `ref` (`Ref`, with `BackRef`, `ArrayBackRef`, `ExternalRef`
  (`_external_ref` for not identifiable references)), `object`
  (`Object`), `array` (`Array`, with `PartialArray`), `partial`
  (`Partial`), `generic` (`Generic`), `inherit` (`Inherit`),
  `denorm` (`Denorm`), `rql` (`RQL`), `json` (`JSON`), `page`
  (`PageType`, the reserved `_page` property, see :doc:`/contrib/architecture/data-access/pagination`).
* `file` (`File`, with `Image`) — file content and metadata, may be
  stored on a separate backend (`_content`, `_content_type`,
  `_bsize`, `_blocks`).
* `geometry` (`Geometry`) and `spatial` (`Spatial`) in
  `spinta/types/geometry/components.py` (PostGIS storage).

Lifecycle
**********

- load — `load_node(context, prop.dtype, data)` resolves the schema and
  `commands.load(Context, prop.dtype, ...)` processes type specific
  manifest fields (e.g. `Object` and `Array` load nested property
  definitions, see `spinta/types/datatype.py`).
- link — references are resolved by the `link` overloads in
  `spinta/types/datatype.py` and `spinta/types/ref/` (e.g. `Ref`
  properties get connected to the referenced model and `_id` keys).
- check — `check(Context, DataType)` is a no-op, with a `Base32`
  override that only allows `base32` on `_id`/`_revision` properties;
  enum values are checked when properties are checked.
- backend commands — types get *per-backend* overloads, e.g. column
  creation (`@commands.get_column.register(PostgreSQL, String)` in
  `spinta/backends/postgresql/commands/column.py`,
  `@commands.get_column.register(Sql, DataType)` in
  `spinta/datasets/backends/sql/commands/column.py`), value casting
  (`@commands.cast_backend_to_python.register(...)` in
  `spinta/datasets/backends/sql/commands/cast.py`), table creation for
  nested types (`@commands.prepare.register(Context, PostgreSQL, Ref)`
  in `spinta/backends/postgresql/types/ref/`), write/read commands in
  `spinta/backends/postgresql/types/<ttype>/` and so on.

External specifications
***********************

- OpenAPI (https://spec.openapis.org/oas/latest.html, local extract
  `.tmp/sources/openapi/OpenAPI.md`): the API layer renders property
  values as a JSON object per UAPI, where `_id`/`_revision` are strings
  and per-property types follow the OAS *Data Types* section (string,
  number, integer, boolean, array, object). Spinta types without an OAS
  primitive are serialized as strings/objects.
- XSD (https://www.w3.org/TR/xmlschema-2/, see `spinta/manifests/xsd/
  helpers.py`): the XSD manifest reader maps XML Schema built-in types
  to Spinta types via `DATATYPES_MAPPING` (e.g. `"decimal"` → `number`,
  `"dateTime"` → `datetime`, `"gYearMonth"` → `date` with an enum
  prepare, `"base64Binary"` → `binary` with base64 prepare). The XSD
  manifest type is read-only (it maps `.xsd` schema files into Spinta
  models).
- RDFS (https://www.w3.org/TR/rdf-schema/, local extract
  `.tmp/sources/w3c/RDF-Schema.md`): property values are `rdfs:Literal`
  nodes typed by datatypes; `rdfs:range` of `rdf:Property` is the
  `Property.dtype` counterpart (`ref` properties correspond to typed
  resource links, see :doc:`properties`).
- Conceptual model (https://raw.githubusercontent.com/atviriduomenys/
  katalogas/f8ad4b7d9f332ed9f9ada6c4e3e735512745e328/uml.drawio.svg, local
  extract `.tmp/sources/atviriduomenys/conceptual-model.md`): the ubiquitous
  language of the Lithuanian open data catalogue uses the same term
  `DataType` — a one-to-one name mapping; there `DataType` is an
  `rdfs:Datatype` referenced by `Property` through a mandatory `type`
  relation (1..1). Spinta's type registry (per-backend commands,
  `ref`/`object`/`array` trees, geometry, files) is richer and stays
  Spinta-specific.
- DSA (https://dsa.readthedocs.io/, local extract `.tmp/sources/dsa/
  DSA.md`): the tabular `property` row's `type` column defines the types
  below; DSA also defines the deprecated `absent`/`temporal`/`spatial`
  types — `temporal` and `spatial` exist in Spinta as aliases
  (`datetime`, `Geometry`), and manifest types are validated against
  `DataTypeEnum`.

See also
********

- :doc:`properties` — the `Property` node that carries `dtype`.
- :doc:`models` — where property types are loaded.
- :doc:`/contrib/architecture/multiple-dispatch/ufuncs/index` — the `prepare` spyna formulas.
- :doc:`/contrib/architecture/data-access/backends/index` — where per-backend type commands live.
- `spinta/types/datatype.py`, `spinta/config.py:CONFIG['components']
  ['types']` — the implementation and the type registry.
