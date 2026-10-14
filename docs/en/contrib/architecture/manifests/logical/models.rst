.. default-role:: literal

*****
Model
*****

`spinta/components.py:Model` is a `MetaData` subclass (see
:doc:`/contrib/architecture/concepts/metadata`) that describes one data model — a named collection of
properties (see :doc:`properties`) served through the API as one resource.
Models are declared in manifests with the ``model`` type (DSA dimension
`model`, see :doc:`/contrib/architecture/manifests/tabular-reader`) and stored in the manifest's
`MetaDataContainer.model` list.

Attributes
**********

- `name` — unique model name, usually a namespaced path, e.g.
  ``/datasets/gov/rc/Report`` (see :doc:`namespaces`).
- `ns` — the `Namespace` this model belongs to. Generated automatically
  from the model name during load (see :doc:`namespaces`).
- `properties` — dict of `Property` instances by property name, including
  reserved properties (`_id`, `_revision`, etc., see :doc:`properties`).
- `flatprops` — flat (top level) properties only; `leafprops` — lists of
  properties of nested types (object, ref, array) keyed by root name.
- `mode` — `Mode.internal` or `Mode.external`; external models are backed
  by an external dataset source (see :doc:`/contrib/architecture/index`).
- `external` — the `Entity` binding to the external dataset source (or
  `None` for internal models).
- `backend` — resolved `Backend` instance: either taken from the model
  definition, inherited from `model.manifest.backend`, or (for external
  models) taken from `model.external.resource.backend`; if nothing is
  available, a `NoBackend` is set.
- `keymap` — the `KeyMap` used to convert source keys to internal UUIDs
  (see :doc:`/contrib/architecture/keymap/index`).
- `base` — the `Base` entry if the model inherits from another model.
- `unique` — list of `unique` constraint property sets.
- `scopes` — `Scope` entries used for authorization (see :doc:`/contrib/architecture/auth/index`
  and `spinta/dimensions/scope/`).
- `access` and `level` — `Access` (`private`/`protected`/`public`/`open`)
  and `Level` (`absent` ... `linked`) maturity enums, both inheritable
  from the external resource (`external.resource.level`,
  `external.resource.access`).
- `status` and `visibility` — lifecycle metadata enums (`develop`,
  `completed`, `deprecated`, ... and `private`/`protected`/`package`/
  `public`).
- `title`, `description`, `uri`, `lang` — descriptive metadata.
- `given` (`ModelGiven`) — values explicitly given in the manifest, e.g.
  `given.pkeys` — the external primary keys from the `pk` column, used
  as default pagination keys.
- `page` — a `PageInfo` pagination helper (see :doc:`/contrib/architecture/data-access/pagination`).
- quick access properties: `id_prop` (`_id`), `revision_prop`
  (`_revision`), `uri_prop` (the `uri` type property, if given).
- `distribution_strategy` — how data is distributed across backends
  (`distribute` config), defaults to `config.default_distribution_strategy`.

Model names must start with an uppercase letter (`spinta/types/
model.py` raises an exception otherwise) and, with `check_names`
enabled, must match the naming rules (`spinta/types/helpers.py:
check_model_name`); the reserved names are `_ns`, `_txn`, `_schema` and
`_schema/Version`.

Model lifecycle
***************

- load — `spinta/types/model.py:load(Context, Model, dict, Manifest)`:
  attaches the model to the manifest, runs `configure` (per-model config
  from `rc` under `models.<name>`), resolves keymap and generates the
  namespace (`load_namespace_from_name`), loads access, level, status and
  visibility, loads all properties via
  `spinta/nodes.py:load_model_properties()` (which first adds the built-in
  reserved properties), creates the `Base` entry, resolves `unique`
  constraints, creates the `Entity` external binding, and builds the
  `PageInfo` via `LoadBuilder.load_page()`.
- link — `spinta/types/model.py:link(Context, Model)`: resolves the
  backend and keymap references, links the external source entity, links
  access inheritance through the namespace chain, links the base and all
  properties, and finally initializes pagination keys with
  `_link_model_page()` (a model without a pagination-capable backend
  gets `page.enabled = False`).
- check — `spinta/types/model.py:check(Context, Model)`: checks the model
  name, requires `_id`, and checks scopes and all properties.
- authorize — `authorize(Context, Action, Model)` calls `authorized()`
  with the model's scopes — this is how UAPI actions are checked against
  a specific model.

Namespace traversal
*******************

Because models are grouped under namespaces, most namespace-level API
operations iterate over models with the `traverse_ns_models` command
(`spinta/manifests/commands/read.py`): it walks `ns.models` recursively
through `ns.names` and yields models that match the action and requested
dataset/resource. This is used by `getall(Context, Namespace, ...)`,
``?all`` requests and `wipe`.

External specifications
***********************

- A model corresponds to a UAPI resource type: URLs address a model by
  name and responses carry the reserved `_type` property filled with
  `node_type()` (https://github.com/atviriduomenys/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`). Pagination per model is described in
  :doc:`/contrib/architecture/data-access/pagination`.
- In DCAT (https://www.w3.org/TR/vocab-dcat-3/, local extract
  `.tmp/sources/w3c/vocab-dcat-3.md`) a model maps to a `dcat:Dataset`
  (or `dcat:Resource`) in a `dcat:Catalog` — `model.title`,
  `model.description` and `model.uri` carry the same descriptive
  metadata as `dcat:title`/`dcat:description`/`dcat:identifier`.
- Conceptual model (https://raw.githubusercontent.com/atviriduomenys/
  katalogas/f8ad4b7d9f332ed9f9ada6c4e3e735512745e328/uml.drawio.svg, local
  extract `.tmp/sources/atviriduomenys/conceptual-model.md`): the ubiquitous
  language of the Lithuanian open data catalogue uses the same term `Model`
  — a one-to-one name mapping; there `Model` is a `sh:NodeShape`, whose
  `visibility`/`status`/`access` enums match the Spinta fields above, and
  `Model` owns `Property` (1..1), points to `Class` (`target`) and to
  `SourceModel` (`source`). Storage, keymap and backend details stay
  Spinta-specific.
- In RDFS (https://www.w3.org/TR/rdf-schema/, local extract
  `.tmp/sources/w3c/RDF-Schema.md`) a model plays the role of `rdfs:Class`
  and its properties of `rdf:Property` instances with `rdfs:domain`.
- DSA (https://dsa.readthedocs.io/, local extract `.tmp/sources/dsa/
  DSA.md`) defines the `model` dimension and the maturity levels that
  load into `Model.level` and `Model.access`.

See also
********

- :doc:`properties` — model properties.
- :doc:`base-models` — model inheritance (`Base`).
- :doc:`namespaces` — the namespace tree models live in.
- :doc:`/contrib/architecture/data-access/pagination` — model pagination helpers (`PageInfo`, `Page`).
- :doc:`/contrib/architecture/data-access/backends/backend` — the `Backend` class referenced by `model.backend`.
- :doc:`/contrib/architecture/concepts/metadata` — where models are stored in the manifest.
