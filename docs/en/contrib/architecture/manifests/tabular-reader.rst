:orphan:

.. default-role:: literal

*************
TabularReader
*************

The readers in `spinta/manifests/tabular/helpers.py` turn tabular
manifest rows (see :doc:`tabular-manifest`) into `(eid, schema)` tuples
consumed by `spinta/manifests/helpers.py:load_manifest_nodes()`. There
is one reader class per dimension, all extending `TabularReader`:

- main dimensions — `ManifestReader`, `DatasetReader`, `ResourceReader`,
  `BaseReader`, `ModelReader`, `PropertyReader`;
- additional dimensions — `AppendReader`, `PrefixReader`,
  `NamespaceReader`, `ParamReader`, `EnumReader`, `LangReader`,
  `CommentReader`, `UniqueReader`, `ScopeReader`.

Reading pipeline
****************

`read_tabular_manifest(format_, path=..., file=...)` dispatches to the
format-specific row reader (`csv`, `ascii`, `xlsx` or `gsheets`), which
yields `(line, cells)` tuples, and feeds them to
`_read_tabular_manifest_rows()`:

#. the first row is normalized and validated by `_detect_header()`
   (columns must be from `spinta/manifests/tabular/constants.py:DATASET`);
#. each subsequent row is zipped into a `ManifestRow` (missing columns
   default to `""`), and the dimension is detected by
   `_detect_dimension()`: the main dimension is the single filled
   dimension column (`dataset`, `resource`, `base`, `model` or
   `property`); otherwise the `type` column must name an additional
   dimension (`prefix`, `enum`, `param`, `comment`, `ns`, `lang`,
   `unique`, `scope` or empty for `AppendReader`);
#. the reader class is looked up in the `READERS` mapping, instantiated
   with the shared `State` and `reader.read(row)` is called;
#. `State.release(reader)` ends (releases) any readers higher in the
   stack — it pops readers while `parent.release(reader)` returns
   `True`, yields their `items()` (the produced schemas) and calls
   `leave()` — and then pushes the new reader via `enter()`.

The same machinery is reused by the `internal` manifest loader
(`spinta/manifests/internal_sql/helpers.py:internal_to_schema()`) with
`allow_updates=True`.

`ParsedRow = Tuple[int, Dict[str, Any]]` is the yielded item type (the
line number or row ID and the schema dict).

The `TabularReader` contract
****************************

`TabularReader` (lines ~190 in `spinta/manifests/tabular/helpers.py`)
holds:

- `state` — the shared `State` object;
- `path`, `line` — location for error messages (`reader.error()` raises
  `TabularManifestError` with `path:line` prefix);
- `type` — the dimension name, `name` — the entry name;
- `data` — the produced schema dict (used when `appendable` is `False`);
- `rows` — list of produced schema dicts (used when `appendable` is
  `True`, e.g. `NamespaceReader`);
- `appendable` — whether more rows can be appended to this reader;
- `allow_updates` — whether the manifest format supports structure
  updates (only the `internal` SQL manifest sets it).

Methods to implement:

- `read(row)` — parse one row into `self.data`;
- `append(row)` — append a continuation row (called for rows routed via
  `AppendReader`); by default it errors when `allow_updates` is `False`;
- `release(reader)` — return `True` when this reader is finished, given
  the next reader type; this drives the stack unwinding;
- `enter()` / `leave()` — register/unregister the reader in `State`
  (e.g. `ModelReader.enter()` sets `state.model = self` and remembers
  the model name in `state.models`);
- `items()` — yield the produced `(line, data)` pairs.

The `State` object keeps the reader `stack` and shortcuts to the current
readers (`manifest`, `dataset`, `resource`, `base`, `model`, `prop`),
the `models` name set (duplicate detection, with optional
`rename_duplicates`) and collected manifest-level `backends`.

Main dimension readers
**********************

- `ManifestReader` — type `manifest`; pushed first for the whole table;
  collects the manifest-level `backends` dict (see `ResourceReader`) and
  the `datasets`/`namespaces` name sets.
- `DatasetReader` — type `dataset`; a `dataset` row with value `/` is a
  section separator (empty data); `type: ns` turns the row into a
  namespace entry; otherwise a `DatasetRow` with `source`, `level`,
  `access`, `title`, `description` etc.
- `ResourceReader` — type `resource`; when no dataset is active, the row
  declares a manifest-level backend (`read_backend()`: `source` is the
  DSN, loaded later by
  `spinta/manifests/helpers.py:_load_manifest_backends()`); otherwise it
  is a `ResourceRow` of the current dataset (`ref` names the backend,
  `source` the external location, `source.type` the original type,
  `prepare` the spyna formula).
- `BaseReader` — type `base`; `BaseRow` linking a model to its parent
  model (`ref` lists the shared primary key properties).
- `ModelReader` — type `model`; builds `ModelRow`: the model name is
  made relative to the current dataset
  (`get_relative_model_name()`), a `/:` suffix carries model features,
  duplicates raise errors or are renamed; `ref` becomes the primary key
  (`unique` and `external.pk`), `source`/`source.type`/`prepare` go into
  `external` (formulas parsed to spyna AST by `_parse_spyna()`); when
  there is no dataset, `resource` names the backend instead.
- `PropertyReader` — type `property`; the most complex reader: parses
  the `type` column into a dtype (`_parse_dtype_string()`: type with
  arguments, `required`/`unique` modifiers), handles nested types
  (`object`, `array`, `ref`, `backref`, `text` — creating intermediate
  parent structures), moves `prepare` into `external.prepare` on
  `leave()` and resolves enum lists.

Additional dimension readers
****************************

- `AppendReader` — type `append`, dimension `""`: a row with no
  dimension column is appended to the current top-of-stack reader
  (`AppendReader.leave()` calls `state.stack[-1].append(self.data)`),
  which is how multi-value rows (e.g. additional `prepare` formulas or
  enum values) are attached to the previous entry.
- `PrefixReader` — type `prefix`; a `PrefixRow` (name in `ref`, `uri`,
  `title`, `description`) attached to the current property, model,
  resource, dataset or the manifest itself.
- `NamespaceReader` — type `ns`, `appendable`; the name is taken from
  the `dataset` column; produces namespace entries.
- `ParamReader` — type `param`; parameter rows for the current node
  (property, model, base, resource, dataset or manifest): `ref` is the
  parameter name, `source`/`prepare` may repeat (appended lists —
  iterators form a Cartesian product, per DSA).
- `EnumReader` — type `enum`; enum value mappings attached to the
  current node; `ref` names the enum list, `source` the original value,
  `prepare` the published value (when `source` is missing, `prepare` is
  used, with the `negative` shortcut handled).
- `LangReader` — type `lang`; `title`/`description` translations
  (`ref` is the ISO 639-1 code) for the current reader — note DSA
  deprecates this dimension, but Spinta still implements it.
- `CommentReader` — type `comment`; comment rows attached to the current
  reader (`ref` the commented property, `source` the author,
  `description` the text).
- `UniqueReader` — type `unique`; multi-column unique constraints on a
  model (`ref` lists the properties, e.g. `prop1,prop2`).
- `ScopeReader` — type `scope`; per-model authorization scopes (`ref`
  the scope name, `prepare` the spyna condition formula; see
  :doc:`/contrib/architecture/multiple-dispatch/ufuncs/ufunc` for the formula machinery).

External specifications
***********************

- DSA (https://dsa.readthedocs.io/, local extract `.tmp/sources/dsa/
  DSA.md`) — the readers implement the DSA table semantics: "only one
  dimension column may be filled per row", the `type` column carrying
  the additional dimensions, the meaning of `ref`/`source`/`prepare` per
  dimension and the reserved `_id`/`_revision` properties. The main
  difference: DSA's `switch` and `migrate` dimensions are not
  implemented in Spinta.
- RML (https://rml.io/specs/rml/, local extract `.tmp/sources/rml/
  RML.md`) — the readers collectively act as RML's *triples map*
  processing: each dimension reader maps source structures (like RML's
  logical source references and join conditions in
  `ResourceReader`/`BaseReader`) to the target model tree.
- UAPI (https://github.com/atviriduomenys/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`) — the `prepare` formulas parsed by
  `_parse_spyna()` end up in `external.prepare` bindings evaluated at
  data extraction time, not in the API layer.

See also
********

- :doc:`tabular-manifest` — the tabular manifest classes and columns.
- :doc:`manifest` — what happens with the produced schemas.
- :doc:`/contrib/architecture/concepts/component` — the spyna formula AST the readers
  produce.
- :doc:`/contrib/architecture/manifests/index` — manifests overview and the load flow.
