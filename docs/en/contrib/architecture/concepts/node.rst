.. default-role:: literal

****
Node
****

`spinta/components.py:Node` is the base class of the manifest node tree —
the tree of typed objects that a manifest is loaded into: namespaces,
models, properties, datasets, etc.

Attributes (all `None` on a fresh node):

- `type` — node type string from the manifest entry, e.g. `"model"`,
  `"ns"`, `"dataset"`.
- `name` — unique node name within its type, e.g.
  `"datasets/ds/Model"`.
- `parent` — the parent node (e.g. the `Property.parent` is its `Model`,
  the `Namespace.parent` is another `Namespace` or the `Manifest`).
- `manifest` — the `Manifest` object this node was loaded from.
- `path` — location of the manifest entry (used for error messages).

Node methods and hashing
************************

- `Node.__repr__()` — `<module.Class(name=...)>` string, also used in
  error messages.
- `Node.__hash__()` — recursive hash over `(type, name, parent)`: nodes
  with the same type and name under different parents stay distinct.
- `Node.node_type()` — returns `Node.type` by default, but subclasses can
  add a specifier, so there can be several node components for the same
  manifest `type` value. For example both `spinta.components.Model` and
  the dataset reader report the `model` type, but
  `node_type()` returns different values (`model` vs `model:dataset`).
  `model_specifier()` provides the specifier (`:ns` for namespaces,
  `:dataset/dsname` for dataset models) and `model_type()` builds the
  full unique name (`name` plus optional `specifier`).
- `Node.basename` — the last `/`-separated fragment of `name`.

Loading the node tree
*********************

Nodes are instantiated and attached to the manifest by
`spinta/nodes.py:get_node()`, which takes the node class from
`config.components['nodes']` (see :doc:`component`), checks `Node.type`
and `name` uniqueness with the `has_node` command and validates the entry
against the class `schema`, then `load_node()` sets the declared
attributes on the instance. When loading completes, the `link` command
connects the nodes together (properties to models, models to namespaces,
references to ref models). Links of the top-level tree are kept in the
manifest's `MetaDataContainer` (see :doc:`metadata`).

`Node` subclasses
*****************

The whole hierarchy is not deep — most of the value comes from two
subclasses declared in the same file:

- `MetaData(Node)` — first-class, named manifest entries: `Namespace`,
  `Model`, `Dataset` (see :doc:`metadata`).
- `ExtraMetaData(Node)` — secondary entries attached to parent nodes:
  `Property`, `Base` etc. They exist as parts of their parent and are not
  stored in the top level manifest objects dict.

External specifications
***********************

- Node = a named, typed resource. In RDF (RDFS,
  https://www.w3.org/TR/rdf-schema/, local extract
  `.tmp/sources/w3c/RDF-Schema.md`) every resource has an `rdf:type` and
  the class hierarchy forms a schema — `Node.type` and the
  `config.components['nodes']` registry play a similar role. SKOS
  (https://www.w3.org/TR/skos-reference/, local extract
  `.tmp/sources/w3c/SKOS.md`): the hierarchical namespaces tree of
  Spinta is similar to `skos:ConceptScheme` with `skos:broader`
  relations.
- In UAPI (https://github.com/atviriduomenys/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`), nodes appear in responses as JSON objects
  with the reserved `_type` property — which is filled from
  `node_type()`.

See also
********

- :doc:`metadata` — manifest metadata entries (`MetaData` and
  `ExtraMetaData`).
- :doc:`/contrib/architecture/manifests/index` — how manifests are read into nodes.
- `spinta/nodes.py` — node loading and linking code.
