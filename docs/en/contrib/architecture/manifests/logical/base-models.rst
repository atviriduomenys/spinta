.. default-role:: literal

****
Base
****

`spinta/components.py:Base` is an `ExtraMetaData` subclass (see
:doc:`/contrib/architecture/concepts/metadata`) that implements model inheritance: it links a child
`Model` (see :doc:`models`) with its parent model and carries a copy of
the parent's primary key properties.

In tabular manifests the `base` dimension is declared with the
`base` column and the `b` short name (see :doc:`/contrib/architecture/manifests/tabular-reader`); the
`BaseReader` (`spinta/manifests/tabular/helpers.py:BaseReader`) reads
the base name and resolves it to a full model name relative to the
current dataset with `get_relative_model_name()`. A base named `/` (an
empty base) means no inheritance.

Attributes
**********

- `name` — base name as given in the manifest.
- `model` — the child model that declares this base (e.g. for
  `a.base.b`, `model` is `b`).
- `parent` — the parent model (e.g. for `a.base.b`, `parent` is `a`);
  as a string until `link`, then a `Model` instance.
- `pk` — list of primary key `Property` instances copied from the
  parent model.
- `level` — `Level` maturity, inherited from
  `external.resource.level`.
- `lang` — language dimension data.

Lifecycle
*********

- load — `spinta/types/model.py:load(Context, Model, dict, Manifest)`
  reads the `base` schema entry of the model and creates the `Base`
  node with `get_node()` (node type `base`), then loads it with
  `commands.load()`; `spinta/types/model.py:load(Context, Base, dict,
  Manifest)` loads the level. In tabular manifests the base is read
  before the model row, so the schema already contains the base
  definition when the model is loaded.
- link — `spinta/types/model.py:link(Context, Base)`: resolves
  `base.parent` with the `get_model` command; if the parent model is
  not declared in the manifest, the base is replaced with a comment
  entry (`replace_undeclared_base_with_comment()`) instead of failing
  the whole manifest load. Then `base.pk` is resolved to the parent
  model properties, and, if the base is identifiable, the primary key
  combination is registered in
  `model.add_keymap_property_combination()` — keymap properties are
  needed for external sources where key values must be encoded into
  internal UUIDs (see :doc:`/contrib/architecture/index`).
- check — `check(Context, Base)` is not registered separately; bases
  are checked through the model (`check(Context, Model)` checks all
  properties, including inherited ones).
- authorization — `spinta/types/model.py:identifiable(Base)` follows
  the same `Level` rules as models.

Inherited properties
********************

Property inheritance is handled by the `inherit` property type
(`spinta/types/inherit/`) and the `_base` reserved property
(see :doc:`properties`): the child model's schema contains inherited
properties resolved through `Base`. Reference (`ref`) properties to
the parent model are created by the `ref` link overloads
(`spinta/types/ref/link.py`).

External specifications
***********************

- DSA (https://dsa.readthedocs.io/, local extract `.tmp/sources/dsa/
  DSA.md`): the `base` dimension with the same `/` empty-base
  convention and primary key copying.
- RDFS (https://www.w3.org/TR/rdf-schema/, local extract
  `.tmp/sources/w3c/RDF-Schema.md`): model inheritance is analogous to
  `rdfs:subClassOf` — the child model specializes the parent and
  inherits its properties.
- In DCAT (https://www.w3.org/TR/vocab-dcat-3/, local extract
  `.tmp/sources/w3c/vocab-dcat-3.md`) there is no direct counterpart —
  dataset specialization is expressed with `dcat:isPartOf` or profiling
  applications instead.

See also
********

- :doc:`models` — child and parent models.
- :doc:`properties` — inherited properties.
- :doc:`/contrib/architecture/manifests/tabular-reader` — the `BaseReader` that reads the `base`
  dimension.
