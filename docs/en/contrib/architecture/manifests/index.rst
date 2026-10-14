.. default-role:: literal

#########
Manifests
#########

In C4 terms (see :doc:`/contrib/architecture/index`), manifest files are one of
Spinta's :ref:`containers <contrib-containers>` (external inputs read by
both the web app and the CLI); this page documents the manifest
:ref:`components <contrib-components>` shared by both.

Manifest is a source, where metadata are read from. Spinta reads manifest
data into an in-memory tree of nodes (namespaces, datasets, models,
properties, etc.) which is then linked, checked and served by the API.

Spinta supports several manifest formats — CSV tables, YAML files, SQL
databases, JSON/XML documents, RDF graphs and others. All manifest classes
live under `spinta/manifests/` and extend `spinta.manifests.components:
Manifest`.

The tabular manifest format (CSV files with `dataset`, `resource`, `base`,
`model`, `property` columns) implements the DSA specification (see
`Duomenų struktūros aprašas (DSA) <https://dsa.readthedocs.io/>`_, local
extract `.tmp/sources/dsa/DSA.md`).

.. image:: /static/manifest-model.png
   :target: /_static/manifest-model.png


Manifest types
**************

Every manifest class has a `type` string (e.g. `"csv"`, `"yaml"`) and is
registered in `spinta/config.py:CONFIG['components']['manifests']`. New
manifest classes are added via configuration, e.g.:

.. code-block:: sh

    SPINTA_COMPONENTS__MANIFESTS__MY=myproject.manifests:MyManifest

Manifest instances are created from configuration: the `manifest` config
option names the default manifest, and the `manifests` section describes
manifests by name, for example:

.. code-block:: yaml

    manifest: default
    manifests:
      default:
        type: csv
        path: manifest.csv
        mode: internal
        keymap: default
        backend: default

See :doc:`/contrib/architecture/configuration/index` for how configuration is read, and `spinta/config.yml`
for the full `manifests` schema (`path`, `file`, `backend`, `keymap`,
`mode` and `sync` options; `switch: type` adds per-type options).

Registered manifest types (as of this writing):

.. list-table::
   :header-rows: 1
   :widths: 10 25 30 35

   * - type
     - class
     - `detect_from_path()`
     - notes
   * - `csv`
     - `spinta.manifests.tabular.components:CsvManifest`
     - `path.endswith('.csv')`
     - tabular CSV file, the DSA table format.
   * - `ascii`
     - `spinta.manifests.tabular.components:AsciiManifest`
     - `path.endswith('.txt')`
     - tabular ASCII table, columns separated by `|`.
   * - `xlsx`
     - `spinta.manifests.tabular.components:XlsxManifest`
     - `path.endswith('.xlsx')`
     - tabular Excel workbook, one sheet per dataset, columns normalized
       to `MANIFEST_COLUMNS`.
   * - `gsheets`
     - `spinta.manifests.tabular.components:GsheetsManifest`
     - URL starts with
       `https://docs.google.com/spreadsheets/`
     - reads a Google Sheets document via the CSV export URL.
   * - `yaml`
     - `spinta.manifests.yaml.components:YamlManifest`
     - (no detection, must be given with explicit type)
     - a directory of YAML files (`*.yml`), one document per version.
   * - `inline`
     - `spinta.manifests.yaml.components:InlineManifest`
     - (never detected)
     - manifest data given inline in configuration
       (`manifests.<name>.manifest`).
   * - `json`
     - `spinta.manifests.dict.components:JsonManifest`
     - `path.endswith('.json')` (but not `openapi+file://`)
     - JSON documents mapped into models.
   * - `xml`
     - `spinta.manifests.dict.components:XmlManifest`
     - `path.endswith('.xml')`
     - XML documents mapped into models.
   * - `openapi`
     - `spinta.manifests.open_api.components:OpenAPIManifest`
     - `path.startswith('openapi+file://')` and
       `path.endswith('.json')`
     - models generated from an OpenAPI specification.
   * - `rdf`
     - `spinta.manifests.rdf.components:RdfManifest`
     - `path.endswith('.rdf')` or `path.endswith('.ttl')`
     - RDF graph (Turtle, etc.) mapped into models.
   * - `xsd`
     - `spinta.manifests.xsd.components:XsdManifest`
     - `path.endswith('.xsd')`
     - XML Schema mapped into models.
   * - `sql`
     - `spinta.manifests.sql.components:SqlManifest`
     - SQLAlchemy DSN whose database does **not** have a `_manifest`
       table
     - models derived from a database schema (tables, columns, foreign
       keys).
   * - `internal`
     - `spinta.manifests.internal_sql.components: InternalSQLManifest`
     - SQLAlchemy DSN whose database **has** a `_manifest` table
     - manifest stored in an internal backend, e.g. for partial
       manifest management; `dynamic = True`, schemas are read with
       `allow_updates = True`.
   * - `backend`
     - `spinta.manifests.backend.components:BackendManifest`
     - (never detected)
     - manifest stored in backend data tables, used for schema version
       synchronization (`spinta bootstrap`, `spinta migrate`).
   * - `memory`
     - `spinta.manifests.memory.components:MemoryManifest`
     - (never detected)
     - empty in-memory manifest, the default, mostly used in tests.

There is also `spinta.manifests.mermaid.components:MermaidManifest`
(`type = "mmd"`, `.mmd` files) — an output-only format used by
`spinta copy -o diagram.mmd` to render a manifest as a Mermaid diagram;
it is not registered in `components.manifests`.

All tabular manifests share the same loading code — see
:doc:`tabular-manifest` and :doc:`tabular-reader`.

The `Manifest` component is described in :doc:`manifest`, and
`ManifestPath` — the parsed manifest location passed around by CLI
commands — in :doc:`manifestpath`.

Logical and physical layers
***************************

Manifest data describes a model in two layers:

- The **logical layer** is what the API serves — namespaces, datasets,
  base models, models, properties and data types (see
  :doc:`logical/index`).
- The **physical layer** binds the logical model to where and how the
  data is physically stored and read — resources, entities and
  attributes (see :doc:`physical/index`).


detect_from_path()
******************

`Manifest.detect_from_path()` is a static method that tells whether a
given path or URL can be read by this manifest class. It is used to
detect the manifest type from a CLI argument, e.g.:

.. code-block:: sh

    spinta copy manifest.csv -o manifest.yml
    spinta copy csv:manifest.csv -o manifest.yml

CLI strings are converted into `ManifestPath` by
`spinta/cli/helpers/manifest.py:convert_str_to_manifest_path()`. If the
argument has an explicit `type:path` prefix and the type is registered
(`spinta/core/config.py:check_if_manifest_valid()`), the type is taken
from the prefix. Otherwise the type is detected by
`spinta/manifests/helpers.py:detect_manifest_from_path()`, which iterates
over all classes registered in `components.manifests` and asks each one
via `detect_from_path()` — the first class that returns `True` wins:

.. code-block:: python

    def detect_manifest_from_path(rc: RawConfig, path: str) -> Type[Manifest]:
        for name in rc.keys("components", "manifests"):
            Manifest_: Type[Manifest] = rc.get("components", "manifests", name, cast=importstr)
            if Manifest_.detect_from_path(path):
                return Manifest_
        raise UnknownManifestTypeFromPath(path=path)

Note that manifests without `detect_from_path()` (the base class returns
`False`), e.g. `yaml`, `memory` or `backend` manifests, can never be
detected — they must be given with an explicit type, or used via
configuration.

New manifest types are detected automatically once their class is
registered in `components.manifests` and implements
`detect_from_path()`.


Loading a manifest
******************

Manifests are loaded at startup (or on demand by CLI commands) as part of
the startup pipeline described in :doc:`/contrib/architecture/wired-together` ("How everything is wired
together"):

#. `spinta/types/store.py:load(Context, Store)` creates the main
   manifest instance with
   `spinta/manifests/helpers.py:create_manifest()` (class taken from
   `manifests.<name>.type` in `components.manifests`) and the internal
   manifest with `create_internal_manifest()` (an `InternalManifest`
   pointing to the packaged `spinta/manifest/` directory with `_ns.yml`,
   `_schema.yml` and `_txn.yml` — the internal models like `_schema` and
   `_txn`).
#. `create_manifest()` initializes the instance
   (`init_manifest()`: `name`, `store`, empty `_objects` lists per node
   type, `prefixes`, `enums`, `access`, `mode`, empty `sync` list), then
   `_configure_manifest()` applies config values: `access`, `keymap`
   (looked up in `store.keymaps`), `backend` (looked up in
   `store.backends`), `mode` and `sync` — each `sync` source is another
   manifest created recursively (with cycle detection). Finally
   `commands.configure(context, manifest)` is called.
#. The `configure` command overload of each manifest type reads
   type-specific options, e.g. `configure(Context, TabularManifest)` in
   `spinta/manifests/tabular/commands/configure.py` reads `path` (and
   `file` for `csv`/`ascii`) from `manifests.<name>` and checks that the
   path exists (`check_manifest_path()`, URLs are allowed too).
#. `commands.load(Context, manifest)` — implemented per manifest type in
   `spinta/manifests/<type>/commands/load.py` — reads primitive schema
   dicts and turns them into nodes. For example, the tabular loader
   (`spinta/manifests/tabular/commands/load.py`) reads rows with
   `spinta/manifests/tabular/helpers.py:read_tabular_manifest()` and
   feeds them to `load_manifest_nodes()`.
#. `spinta/manifests/helpers.py:load_manifest_nodes()` walks the
   `(eid, schema)` iterator: entries with `type == 'manifest'` configure
   manifest-level `backends`, `prefixes` and `enums`; every other entry
   is turned into a node via `spinta/nodes.py:get_node()` (class from
   `config.components['nodes']`, see :doc:`/contrib/architecture/concepts/node`), loaded with
   `commands.load(Context, node, data, manifest)` and stored with
   `commands.set_node()`. At the end the root namespace is ensured to
   exist.
#. After loading, the store runs `commands.link(Context, store.manifest)`
   (resolves references between nodes) and
   `commands.check(Context, store.manifest)` (validates definitions).

The `load` command has a few extra knobs:

- `into` — load one manifest *into* another, e.g. the internal manifest
  is loaded into the main manifest to provide the `_schema` model, and
  `spinta` commands that need a full picture load `sync` sources into the
  main manifest.
- `load_internal` — load the internal manifest into the target if the
  `_schema` model is not yet present.
- `rename_duplicates` — instead of raising an error on duplicate node
  names, add a numeric suffix.
- `full_load` — for `internal` manifests: read the full schema from the
  database instead of the initial schema.

The `sync` option makes a manifest read its nodes from other manifests
(e.g. a `backend` manifest syncs schema versions from a `csv` source
manifest) — `Manifest.sync` is a list of manifests created by
`_configure_manifest()` and loaded by each `load` implementation after
the manifest's own nodes.


Adding a new manifest type
**************************

New manifests are added under `spinta/manifests` directory.

`components.py` file should contain your manifest class component, for
example:

.. code-block:: python

    from spinta.manifests.components import Manifest


    class MyManifest(Manifest):
        type: str = 'my'

        @staticmethod
        def detect_from_path(path: str) -> bool:
            return path.endswith('.my')

Implement at least the `configure` and `load` commands (they get
autodiscovered via `spinta.manifests` being listed in
`CONFIG['commands']['modules']`, see :doc:`/contrib/architecture/multiple-dispatch/commands`):

.. code-block:: python

    # spinta/manifests/my/commands/configure.py

    from spinta import commands
    from spinta.components import Context
    from spinta.manifests.my.components import MyManifest


    @commands.configure.register(Context, MyManifest)
    def configure(context: Context, manifest: MyManifest):
        rc = context.get("rc")
        manifest.path = rc.get("manifests", manifest.name, "path")

.. code-block:: python

    # spinta/manifests/my/commands/load.py

    from spinta import commands
    from spinta.components import Context
    from spinta.manifests.components import Manifest
    from spinta.manifests.helpers import load_manifest_nodes
    from spinta.manifests.my.components import MyManifest
    from spinta.manifests.my.helpers import read_my_manifest


    @commands.load.register(Context, MyManifest)
    def load(
        context: Context,
        manifest: MyManifest,
        *,
        into: Manifest = None,
        rename_duplicates: bool = False,
        load_internal: bool = True,
        full_load: bool = False,
    ):
        if load_internal:
            target = into or manifest
            if not commands.has_model(context, target, "_schema"):
                store = context.get("store")
                commands.load(context, store.internal, into=target)

        if manifest.path is None:
            return

        schemas = read_my_manifest(manifest.path, rename_duplicates=rename_duplicates)

        if into:
            load_manifest_nodes(context, into, schemas, source=manifest)
        else:
            load_manifest_nodes(context, manifest, schemas)

        for source in manifest.sync:
            commands.load(
                context, source,
                into=into or manifest,
                rename_duplicates=rename_duplicates,
                load_internal=load_internal,
                full_load=full_load,
            )

Here, you need to implement `read_my_manifest` function, which is
responsible for producing iterator with following items:

.. code-block:: python

   (eid, schema)

`eid` is a manifest entry ID (`spinta/components.py:EntryId` alias):
a line number, database primary key, file path or other identifier, where
this manifest item can be located. It is used in error messages and as a
schema version ID for backend manifests.

`schema` is a dict, that must contain at least `type` key. `type` must be
defined in `spinta.config:CONFIG['components']['nodes']`, which at the
time of writing this text, looks like this:

.. code-block:: python

    {
        'ns': 'spinta.components:Namespace',
        'model': 'spinta.components:Model',
        'dataset': 'spinta.datasets.components:Dataset',
        'base': 'spinta.components:Base',
    }

Components can be added via configuration, for example, using environment
variables:

.. code-block:: sh

    SPINTA_COMPONENTS__NODES__MY=myproject.components:My

See :ref:`configuration` for more information.

Each component, has `schema` attribute, where you can find, what can be
used in `schema` for each `type`.

Here is an example for `type: model`:

.. code-block:: python

    from spinta.utils.schema import NA

    {
        'type': 'model',
        'name': 'City',
        'title': 'City',
        'description': '',
        'external': {
            'dataset': 'example/dataset',
            'resource': 'myres',
            'pk': ['id'],
            'name': 'my://cities',
            'prepare': NA,
        },
        'properties': {
            'id': {
                'type': 'integer',
                'type_args': None,
                'required': True,
                'unique': True,
                'external': {
                    'name': 'ID',
                    'prepare': NA,
                }
            },
        },
    }

A good place to look for examples, would be
`spinta/manifests/tabular/helpers.py`_, look for `*Reader` classes (see
also :doc:`/contrib/architecture/manifests/tabular-reader`).

.. _spinta/manifests/tabular/helpers.py: https://github.com/atviriduomenys/spinta/blob/master/spinta/manifests/tabular/helpers.py

Once you produce schemas as described above, then `load_manifest_nodes`
function will take care of everything else.

In addition, if your manifest format is tabular (row based), consider
implementing the reverse direction — rendering the manifest back to rows
— so it can be used as an output format of `spinta copy` (see
`spinta/manifests/tabular/helpers.py:write_tabular_manifest()` and
`spinta/manifests/mermaid/helpers.py:write_mermaid_manifest()` for
examples).


`spinta copy` and `spinta inspect`
**********************************

Two CLI commands work directly with manifests:

- `spinta copy` (`spinta/cli/manifest.py`) — copies models between
  manifests, optionally transforming them (access filtering by the
  `access` column, name reformatting, column selection, ordering,
  deduplication). The output format is detected from the output file
  extension: `.csv`/`.xlsx` (via `write_tabular_manifest()`),
  `internal_sql` DSN (via `write_internal_sql_manifest()`), `.mmd`
  (via `write_mermaid_manifest()`), or an ASCII table printed to
  stdout.
- `spinta inspect` (`spinta/cli/inspect.py`) — reads an external data
  source (given as a manifest path or via `-r type uri` /
  `-b dsn` options) and creates or updates a manifest with models
  derived from that source
  (`spinta/datasets/inspect/helpers.py:create_manifest_from_inspect()`),
  printing the result as an ASCII table or writing it with an exporter
  (see `config.exporters`: `csv`, `xlsx`, `json`, `jsonl`, `html`,
  `rdf`, `ascii`).

These commands build the manifest address via `ManifestPath` (see
:doc:`/contrib/architecture/manifests/manifestpath`) and load manifests through the same
`commands.load` machinery described above.


External specifications
***********************

- DSA (https://dsa.readthedocs.io/, local extract
  `.tmp/sources/dsa/DSA.md`) — the tabular manifest format is an
  implementation of the DSA data structure description table: the
  `dataset`, `resource`, `base`, `model` and `property` dimensions and
  the metadata columns (`id`, `type`, `ref`, `source`, `source.type`,
  `prepare`, `level`, `access`, `uri`, `title`, `description`, ...) map
  one to one to the columns read by the tabular readers. Note that DSA
  also defines the `prefix`, `enum`, `param`, `comment` additional
  dimensions (implemented in Spinta, see
  :doc:`/contrib/architecture/manifests/tabular-reader`) as well as `lang` and `migrate`
  (deprecated in DSA; Spinta implements `lang`, but not `migrate` or
  DSA's `switch` dimension).
- RML (https://rml.io/specs/rml/, local extract
  `.tmp/sources/rml/RML.md`) — the RDF Mapping Language generalizes
  mapping of heterogeneous structured sources to RDF. Spinta manifests
  play a similar role for the `rdf`, `json`, `xml`, `sql` and `xsd`
  manifest types: a manifest entry maps an external source location
  (`model.external.name`, like RML's `rml:reference`/`rr:template`) and
  `prepare` formulas to a model in Spinta's internal model tree. The
  main difference is direction: RML produces RDF triples, Spinta
  produces `Model`/`Property` nodes that are then served via UAPI.
- UAPI (https://github.com/atviriduomenys/uapi, local extract
  `.tmp/sources/uapi/UAPI.md`) — manifests define models which the API
  serves; reserved properties (`_id`, `_revision`, `_type`) and access
  levels originate from manifest data.


See also
********

- :doc:`/contrib/architecture/manifests/manifest` — the `Manifest` component.
- :doc:`/contrib/architecture/manifests/manifestpath` — `ManifestPath`, the parsed manifest
  location.
- :doc:`/contrib/architecture/manifests/tabular-manifest` — tabular manifest classes
  (`CsvManifest`, `AsciiManifest`, `XlsxManifest`, `GsheetsManifest`).
- :doc:`/contrib/architecture/manifests/tabular-reader` — the tabular readers
  (`TabularReader` and its subclasses).
- :doc:`/contrib/architecture/data-access/backends/external-data-sources` — the dataset layer (`Dataset`, `Resource`, `Entity`,
  `Attribute`) that manifest rows describing external data load into.
- :doc:`/contrib/architecture/wired-together` — the startup pipeline that loads manifests.

.. toctree::
   :maxdepth: 1

   Logical layer <logical/index>
   Physical layer <physical/index>



