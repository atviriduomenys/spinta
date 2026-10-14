.. default-role:: literal

*********
Component
*********

`spinta/components.py:Component` is the base class of most Spinta building
blocks. Subclasses are instantiated with configuration values and used as
plain typed objects (datacontainers) throughout the code.

`Component` itself declares only one class attribute:

- `schema = {}` — a dict that declares which attributes of the component
  can be loaded from manifest data. Each schema entry describes one
  attribute, e.g. `{'title': {}}` declares an optional `title` attribute,
  while `{'type': {'type': 'string', 'required': True}}` declares a
  required string attribute. The same schema language is also used to
  declare configuration options in `spinta/config.yml` (see
  :doc:`/contrib/architecture/configuration/index`).

At runtime schema entry values are consumed by `load_node()`
(`spinta/nodes.py`) and the typed `load` command overloads: required
attributes must be present, missing values get schema `default` or
`factory`, `inherit` entries are taken from the parent node, and unknown
keys raise `UnknownParameter`. Schemas are merged down the class
hierarchy with `spinta/utils/schema.py:resolve_schema()`.

Component instances vs component classes
****************************************

Two different mechanisms share the word *component*:

- `config.components` — a registry of component *classes*
  (`components.core`, `components.manifests`, `components.backends`,
  `components.nodes`, `components.types`, `components.datasets`,
  `components.keymaps`, ..., all declared in `spinta/config.py:CONFIG`
  under the `components` section). Manifest loader
  `spinta/nodes.py:get_node()` picks the class of a new node from
  `config.components['nodes'][ctype]`, and backends, keymaps, query
  builders, etc. are picked from their own groups. Because the registry
  is a config option, replacements can be made by configuration alone.
- `Component` subclasses — the actual objects created from those classes:
  `Node` and its subclasses (`MetaData`, `ExtraMetaData`, `Model`,
  `Property`, `Namespace`, ...), `Manifest` subclasses, `Backend`
  subclasses, `Format` renderers, `KeyMap` implementations and `DataType`
  subclasses.

Some important classes intentionally do *not* extend `Component`, e.g.
`Context`, `Store` and `Config` are plain classes instantiated in
`spinta/core/context.py:create_context()` and loaded by the `load`
command (see :doc:`/contrib/architecture/configuration/index` and :doc:`/contrib/architecture/configuration/store`).

External specifications
***********************

- The `schema` dict plays the same role as the OpenAPI Schema Object
  (https://spec.openapis.org/oas/latest.html, local extract
  `.tmp/sources/openapi/OpenAPI.md`): it declares the data model of an
  object — names, types, requiredness and defaults — used both for
  validating incoming manifest data and for describing the API.
- In the DSA tabular manifest specification
  (https://dsa.readthedocs.io/, local extract `.tmp/sources/dsa/DSA.md`)
  manifest dimensions (`dataset`, `resource`, `model`, `property`, ...)
  load into separate component classes — one schema per dimension.
- DCAT (https://www.w3.org/TR/vocab-dcat-3/, local extract
  `.tmp/sources/w3c/vocab-dcat-3.md`) models a catalog by a closed set of
  classes (`dcat:Dataset`, `dcat:Distribution`, ...); Spinta does the
  same with node components registered in `config.components['nodes']`.

See also
********

- :doc:`node` — the manifest node components.
- :doc:`/contrib/architecture/configuration/store` — the `Store` container.
- :doc:`/contrib/architecture/configuration/index` — how component classes are configured.
