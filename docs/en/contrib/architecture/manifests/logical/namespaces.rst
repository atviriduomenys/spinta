.. default-role:: literal

*********
Namespace
*********

`spinta/components.py:Namespace` is a `MetaData` subclass (see
:doc:`/contrib/architecture/concepts/metadata`) — a tree node that groups models (see :doc:`models`)
into a hierarchy. The namespace tree is used to organize model URLs and
to give access levels and backends to whole groups of models at once.

Namespace nodes are generated automatically from model names: when a
model `/datasets/gov/rc/Report` is loaded, the
`load_namespace_from_name()` function (`spinta/types/namespace.py`)
creates namespaces `""` (root), `"datasets"`, `"datasets/gov"` and
`"datasets/gov/rc"` and registers the model in the leaf namespace
(`ns.models`). Namespaces can also be declared explicitly in manifests
with the `ns` type (the `NamespaceReader` in :doc:`/contrib/architecture/manifests/tabular-reader`).

Attributes
**********

- `name` — the namespace path, e.g. `"datasets/gov/rc"`.
- `parent` — the parent `Namespace`, or the `Manifest` for the root
  namespace (`Namespace.is_root()`).
- `names` — dict of child namespaces by name; `models` — dict of models
  registered directly in this namespace.
- `title`, `description` — descriptive metadata (`NamespaceData`).
- `access` — `Access` level inherited by models under this namespace
  unless overridden.
- `keymap` and `backend` — optional defaults for models in this
  namespace.
- `generated` — `True` when the namespace was generated from a model
  name rather than declared in a manifest.
- `given` (`NamespaceGiven`) — explicitly given values (`access`).
- `enums` — enum storage from the `enum` dimension.

Loading and linking
********************

- `load(Context, Namespace, dict, Manifest)` (`spinta/types/
  namespace.py`) sets `ns.names`/`ns.models` to empty dicts and
  registers the namespace in the manifest with the `set_namespace`
  command.
- `link(Context, Namespace)` links the parent namespace by splitting
  the name and registers this namespace in the parent's `names` dict.
- `check(Context, Namespace)` validates the namespace name when
  `config.check_names` is enabled (the reserved name `_schema` is
  always allowed).

Serving namespaces over the API
*******************************

`spinta/types/namespace.py` also implements the API commands for
namespace URLs:

- `getall(Context, Namespace, Request)` authorizes and returns the
  namespace content — either models listed in the namespace or, with
  ``?all``, all rows of all models under it, traversed recursively with
  `traverse_ns_models` (see :doc:`models`). Namespace requests only
  report models the user is authorized to access.
- `getone(Context, Request, Namespace)` raises `ModelNotFound` —
  namespaces themselves have no data.
- `wipe(Context, Namespace, None)` wipes data of all models under the
  namespace, ordered topologically by references.

External specifications
***********************

- SKOS (https://www.w3.org/TR/skos-reference/, local extract
  `.tmp/sources/w3c/SKOS.md`): the namespace tree is analogous to a
  `skos:ConceptScheme` of `skos:Concept` nodes with `skos:broader` relations
  — a hierarchy of groupings, with the root namespace playing the role
  of the scheme and child namespaces of narrower concepts.
- UAPI (https://github.com/atviriduomenys/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`): namespaces appear as URL path prefixes
  of model URLs; a namespace URL lists the models under it, and with
  the ``all()`` operator returns data of all models recursively. In the
  UAPI 2.0 draft namespaces are modelled as URL segments — current
  Spinta serves namespace listing at the namespace URL directly (verify
  in `spinta/api/__init__.py:homepage`).
- DCAT (https://www.w3.org/TR/vocab-dcat-3/, local extract
  `.tmp/sources/w3c/vocab-dcat-3.md`): a namespace corresponds to a
  `dcat:Catalog` (or a sub-catalog) containing `dcat:Dataset` entries
  (models).
- In RDFS (https://www.w3.org/TR/rdf-schema/, local extract
  `.tmp/sources/w3c/RDF-Schema.md`) namespaces are related to RDF
  vocabulary namespaces (URI prefixes), but Spinta namespaces are
  concrete tree nodes, not just URI prefixes.

See also
********

- :doc:`models` — models registered in namespaces.
- :doc:`/contrib/architecture/concepts/metadata` — how namespaces are stored in manifests.
- :doc:`/contrib/architecture/concepts/node` — the base class of the node tree.
