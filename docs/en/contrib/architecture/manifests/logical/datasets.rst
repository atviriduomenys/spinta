.. default-role:: literal

*******
Dataset
*******

`spinta/datasets/components.py:Dataset` is a `MetaData` subclass (see
:doc:`/contrib/architecture/concepts/metadata`) describing one external dataset: DCAT-compatible metadata
plus a set of `resources` from which the data is actually read. Datasets are
declared in manifests with the ``dataset`` type (DSA dimension `dataset`) and
stored in the manifest's `MetaDataContainer.dataset` list.

Attributes
**********

- `name` — dataset name, doubles as the namespace name: dataset load
  generates a `Namespace` with `load_namespace_from_name()` (see
  :doc:`namespaces`), so ``/datasets/gov/rc`` is both a namespace and a
  dataset.
- `manifest` — the parent :doc:`/contrib/architecture/manifests/manifest` (set by `load_node`).
- `ns` — the generated `Namespace`.
- `resources` — dict of `Resource` instances by name (see
  :doc:`/contrib/architecture/manifests/physical/resources`), created while loading the dataset row.
- `level` — `Level` maturity of the data, default `3` (`open`); `access` —
  default `Access.private`.
- `website`, `source` — informational URL and metadata source.
- `title`, `description` — descriptive metadata (copied to the generated
  namespace if it was not explicitly declared).
- `count` — optional object count.
- `prefixes` — `UriPrefix` entries from the ``prefix`` dimension.
- `lang` — `LangData` translations from the ``lang`` dimension.
- `given` (`DatasetGiven`) — values given in the manifest that are not part
  of the schema (`access`, `given_name`).

The `schema` also declares optional `version` (a `number`/`date` pair),
`owner` and `projects` references into the manifest's node metadata.

Dataset lifecycle
*****************

- load — `spinta/datasets/commands/load.py:load(Context, Dataset, dict,
  Manifest)`: generates or updates the namespace, loads prefixes into
  `dataset.prefixes` and enums into `ns.enums`, then loads each entry of
  `resources` as a `Resource` node via `get_node(..., group="datasets",
  ctype="resource")` and `commands.load`.
- link — `link(Context, Dataset)`: links access through the manifest chain
  (default access level from `config.default_access_level`) and links all
  resources.
- check — `check(Context, Dataset)`: validates the name with
  `spinta/utils/naming.py:is_valid_namespace_name()` when `check_names` is
  enabled, and checks all resources.
- identifiable — `commands.identifiable(Dataset)` returns `True` when
  `dataset.level` is unset or at least `Level.identifiable` (4): only
  identifiable datasets get keymap entries during sync.

External specifications
***********************

- DCAT (https://www.w3.org/TR/vocab-dcat-3/, local extract
  `.tmp/sources/w3c/vocab-dcat-3.md`): a `Dataset` corresponds to a
  `dcat:Dataset` in a `dcat:Catalog` — `dataset.title`, `description`,
  `website` and `version` carry the same kind of descriptive metadata as
  `dct:title`, `dct:description` and `dcat:version`.
- Conceptual model (https://raw.githubusercontent.com/atviriduomenys/
  katalogas/f8ad4b7d9f332ed9f9ada6c4e3e735512745e328/uml.drawio.svg, local
  extract `.tmp/sources/atviriduomenys/conceptual-model.md`): the ubiquitous
  language of the Lithuanian open data catalogue also uses the term
  `Dataset` — a one-to-one name mapping; `Dataset` there is a
  `dcat:Dataset`, matching the DCAT anchor above.
- DSA (https://dsa.readthedocs.io/, local extract `.tmp/sources/dsa/DSA.md`)
  defines the `dataset` dimension (`name`, `version`, `level`, `access`,
  `title`, `description`, `website`, ...) that loads into this class; see
  :doc:`/contrib/architecture/manifests/tabular-reader` for how rows are parsed.

See also
********

- :doc:`/contrib/architecture/manifests/physical/resources` — the data distributions of a dataset.
- :doc:`namespaces` — the namespace generated from the dataset name.
- :doc:`/contrib/architecture/data-access/backends/external-data-sources` — the datasets layer overview.
