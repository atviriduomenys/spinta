.. default-role:: literal

********
Resource
********

`spinta/datasets/components.py:Resource` is an `External` (an
`ExtraMetaData` subclass, see :doc:`/contrib/architecture/concepts/metadata`) describing one accessible form
of a :doc:`../logical/datasets`'s data: which backend to use, where the data lives and
how to reach it. In tabular manifests a resource is declared with the
``resource`` dimension and, when it defines a backend, doubles as the
``backend`` dimension (see :doc:`/contrib/architecture/manifests/tabular-reader`).

Attributes
**********

- `name` — resource name, set from the manifest row name.
- `dataset` — the parent `Dataset`.
- `backend` — the resolved `ExternalBackend` instance (see
  :doc:`/contrib/architecture/data-access/backends/external-data-sources`). Resolution happens at load time in
  `spinta/datasets/helpers.py:load_resource_backend()`: a `ref` to a
  manifest or configured backend, an inline backend built from `type` and
  `external` (dsn), or a fallback to the manifest backend.
- `external` — the source location: DSN, URL or file path (the ``source``
  column in tabular manifests). May contain ``{placeholders}`` filled from
  `resource.params`; the used placeholder names are collected into
  `resource.source_params`.
- `type` — backend type name (e.g. `"sql/postgresql"`, `"dask/csv"`).
- `prepare` — a spyna formula (see :doc:`/contrib/architecture/multiple-dispatch/ufuncs/expr` and :doc:`/contrib/architecture/multiple-dispatch/ufuncs/ufunc`) merged with
  model and user queries when reading from this resource.
- `level` and `access` — `Level`/`Access` with `inherit: dataset.level` /
  `dataset.access`, so resources inherit the dataset's maturity and access
  unless overridden.
- `models` — dict filled at link time with the models bound to this resource
  (models register themselves via their `Entity`, see :doc:`entities`).
- `params` — `Param` objects (source values and `prepare` formulas),
  `source_params` — set of placeholder names used in `external`.
- `title`, `description`, `lang`, `comments` — descriptive metadata.
- `given` (`ResourceGiven`) — `access` and `given_name` as given in the
  manifest.

The `schema` also declares `backend` as a reference to
`context.store.backends` — that is how a `ref` column value links to a
backend defined earlier in the manifest or in the configuration.

Resource lifecycle
******************

- load — `spinta/datasets/commands/load.py:load(Context, Resource, dict,
  Manifest)`: parses `prepare` from spyna to an `Expr`, loads access (with
  inheritance from the dataset), lang data and comments, resolves the
  backend (`load_resource_backend`, skipped when `config.ensure_backends`
  is off and the backend is not configured), collects `source_params` from
  the `external` placeholders and loads `params`.
- link — `link(Context, Resource)`: links access, links params via
  `link_params()` and runs `LinkBuilder.resolve()` on the resource
  `prepare` formula.
- check — `check(Context, Resource)` is a no-op.
- identifiable — like datasets, resources take part in
  `commands.identifiable()`: level >= `Level.identifiable` (4) means
  keymap-encoded identifiers are produced during sync.
- `get_param_http_headers()` collects HTTP headers produced by
  ``header()`` formulas in params (used by SOAP/HTTP backends).

External specifications
***********************

- DCAT (https://www.w3.org/TR/vocab-dcat-3/, local extract
  `.tmp/sources/w3c/vocab-dcat-3.md`): a resource corresponds to a
  `dcat:Distribution` (or, for service-like sources, `dcat:DataService`) of
  a `dcat:Dataset` — `resource.external` plays the role of
  `dcat:accessURL`/`dcat:downloadURL` and `resource.backend` the access
  service.
- RML (https://rml.io/specs/rml/, local extract `.tmp/sources/rml/RML.md`):
  a resource corresponds to an RML *logical source*
  (`rml:source` + `rml:referenceFormulation`): it says where the input rows
  come from and how they are read, while :doc:`entities` and
  :doc:`attributes` say how rows map to the target model.
- Conceptual model (https://raw.githubusercontent.com/atviriduomenys/
  katalogas/f8ad4b7d9f332ed9f9ada6c4e3e735512745e328/uml.drawio.svg, local
  extract `.tmp/sources/atviriduomenys/conceptual-model.md`): the ubiquitous
  language of the Lithuanian open data catalogue maps the resource to
  `Distribution` — the data access point of a dataset (there
  `Dataset` has 0..* `Distribution` relations, paralleling the
  `dcat:Distribution` reading above; service-like sources correspond to
  `DataService`).
- DSA (https://dsa.readthedocs.io/, local extract `.tmp/sources/dsa/DSA.md`)
  defines the `resource`/`backend` dimensions and the source types
  (`sql`, `csv`, `wsdl`, ...) whose type names are backend registry keys.

See also
********

- :doc:`../logical/datasets` — the parent dataset.
- :doc:`entities` — how models bind to a resource.
- :doc:`/contrib/architecture/data-access/backends/external-data-sources` — external backends and the read path.
