:orphan:

.. default-role:: literal

***************
TabularManifest
***************

`spinta/manifests/tabular/components.py:TabularManifest` is the base
class of the tabular manifests — manifests stored as tables with the DSA
dimensions as columns. Tabular manifests implement the DSA data
structure description (https://dsa.readthedocs.io/, local extract
`.tmp/sources/dsa/DSA.md`) and are the most common way to describe
datasets in Spinta.

The base class only declares the shared bits:

.. code-block:: python

    class TabularFormat(Enum):
        CSV = "csv"
        ASCII = "ascii"
        XLSX = "xlsx"
        GSHEETS = "gsheets"

    class TabularManifest(Manifest):
        format: TabularFormat = None
        path: str = None

Concrete formats
****************

.. list-table::
   :header-rows: 1
   :widths: 22 18 30 30

   * - class
     - `format`
     - `detect_from_path()`
     - read by
   * - `CsvManifest`
     - `TabularFormat.CSV`
     - `path.endswith('.csv')`
     - `spinta/manifests/tabular/helpers.py:_read_csv_manifest()`
       (python `csv` module, `utf-8`); `CsvManifest.file` can hold an
       open file object (stdin or piped manifests).
   * - `AsciiManifest`
     - `TabularFormat.ASCII`
     - `path.endswith('.txt')`
     - `spinta/manifests/tabular/helpers.py:_read_txt_manifest()` →
       `_read_ascii_tabular_manifest()` (columns separated by `|`,
       column short names `d`/`r`/`b`/`m`/`p`/`t` are expanded by
       `normalizes_columns()`).
   * - `XlsxManifest`
     - `TabularFormat.XLSX`
     - `path.endswith('.xlsx')`
     - `spinta/manifests/tabular/helpers.py:_read_xlsx_manifest()`
       (`openpyxl`); one sheet per dataset, header row normalized to
       `DATASET` columns, sheets with more than 100 consecutive empty
       rows are cut off.
   * - `GsheetsManifest`
     - `TabularFormat.GSHEETS`
     - URL starts with `https://docs.google.com/spreadsheets/`
     - `spinta/manifests/tabular/formats/gsheets.py: read_gsheets_manifest()`
       — extracts the sheet key (and optional
       `gid`) from the URL and reads the CSV export endpoint
       (`gviz/tq` with `tqx: out:csv`).

All four formats are dispatched by
`spinta/manifests/tabular/helpers.py:read_tabular_manifest(format_)`,
which yields rows in a single internal representation and feeds them to
the readers (see :doc:`tabular-reader`).

Columns
*******

The columns of a tabular manifest are defined in
`spinta/manifests/tabular/components.py` (mirrored by
`spinta/manifests/tabular/constants.py:DATASET`):

- dimension columns — `id`, `dataset`, `resource`, `base`, `model`,
  `property` (only one of `dataset`/`resource`/`base`/`model`/`property`
  may be filled per row);
- metadata columns — `type`, `ref`, `source`, `source.type`, `prepare`,
  `origin`, `count`, `level`, `status`, `visibility`, `access`, `uri`,
  `eli`, `title`, `description`.

The columns are typed as the `ManifestColumn` `Literal` and a row is a
`ManifestRow` (a `Dict[ManifestColumn, str]`). The `_detect_header()` /
`normalizes_columns()` helpers validate and normalize the header row.

Row structures
**************

Each dimension has a typed dict describing what the readers produce:

- `ManifestTableRow` — the manifest-level entry (`type`, `backends`).
- `DatasetRow` — a dataset (with nested `resources`), or a namespace
  when `type` is `ns`.
- `ResourceRow` — a resource of a dataset; `BackendRow` — a manifest
  level backend (a `resource` row outside any dataset declares a
  backend).
- `BaseRow` — a base model entry for model inheritance.
- `ModelRow` — a model, with nested `properties` (`PropertyRow`),
  `scopes` (`ScopeRow`) and `params` (`ParamRow` via `ModelExtraData`);
  `ModelExternalRow` describes the external source binding.
- `PropertyRow` — a property, with nested `enums` (`EnumRow`);
  `PropertyExternalRow` describes the external column binding.
- `PrefixRow`, `CommentRow` — additional dimension rows.

These dicts are the `(eid, schema)` items produced by the readers and
consumed by `spinta/manifests/helpers.py:load_manifest_nodes()` (see
:doc:`manifest`).

Loading and writing
*******************

The `load` command overload for all tabular manifests lives in
`spinta/manifests/tabular/commands/load.py` (registered for
`TabularManifest`, so `csv`, `ascii`, `xlsx` and `gsheets` share it); it
ensures the internal manifest is loaded (`_schema` model), reads rows
with `read_tabular_manifest()` and calls `load_manifest_nodes()`.

For writing, `spinta/manifests/tabular/helpers.py:write_tabular_manifest()`
renders rows back to `.csv` or `.xlsx` files, and
`datasets_to_tabular()` converts a loaded manifest (models, datasets,
resources, prefixes, enums, ...) into `ManifestRow` rows — this is what
`spinta copy` uses (see :doc:`/contrib/architecture/manifests/index`).

External specifications
***********************

- DSA (https://dsa.readthedocs.io/, local extract `.tmp/sources/dsa/
  DSA.md`) — the tabular manifest implements the DSA table: dimension
  columns `dataset`/`resource`/`base`/`model`/`property`, metadata
  columns and the additional dimensions given in the `type` column
  (`prefix`, `enum`, `param`, `comment`, ...). DSA deprecates the `lang`
  and `migrate` dimensions — Spinta still implements `lang` (see
  :doc:`tabular-reader`), while `migrate` and DSA's `switch` dimension
  are not implemented.
- RML (https://rml.io/specs/rml/, local extract `.tmp/sources/rml/
  RML.md`) — as with all manifests, tabular manifest rows are mapping
  rules: the `source` column is the physical source location (like
  RML's `rml:reference`), and `prepare` formulas adapt the source values
  to the published model.
- RFC 4180 — DSA requires machine-readable manifests to be UTF-8 CSV;
  `CsvManifest` reads CSV accordingly.

See also
********

- :doc:`tabular-reader` — how tabular rows are parsed into schemas.
- :doc:`manifest` — the `Manifest` base class.
- :doc:`/contrib/architecture/manifests/index` — manifests overview and load flow.
