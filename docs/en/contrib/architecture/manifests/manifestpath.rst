:orphan:

.. default-role:: literal

************
ManifestPath
************

`spinta/manifests/components.py:ManifestPath` is a small dataclass that
represents the *location* of a manifest — not the manifest itself. While
`Manifest` (see :doc:`manifest`) is the loaded component, `ManifestPath`
is the address that is passed around before anything is loaded: CLI
arguments, test helpers and configuration builders all speak in
`ManifestPath` terms.

.. code-block:: python

    @dataclasses.dataclass
    class ManifestPath:
        type: str = "tabular"
        name: str | None = None
        path: str | None = None
        file: Optional[IO] = None
        prepare: str | None = None

Fields
******

- `type` — manifest type name, e.g. `"csv"`, `"ascii"`, `"internal"`;
  must be one of the types registered in
  `spinta/config.py:CONFIG['components']['manifests']`.
- `name` — optional manifest name (used e.g. for `sync` entries).
- `path` — file path, URL or DSN.
- `file` — an open file-like object (e.g. `io.StringIO`); used when the
  manifest is piped in instead of read from disk, e.g. in tests
  (`spinta/testing/manifest.py` wraps ASCII tables into
  `ManifestPath(type='ascii', file=StringIO(...))`) and by
  `spinta copy`/`spinta import` reading from stdin.
- `prepare` — optional prepare formula string.

Creating manifest paths
***********************

CLI strings are converted to `ManifestPath` by
`spinta/cli/helpers/manifest.py:convert_str_to_manifest_path()`:

#. if the string has an `type:path` prefix (a colon not followed by a
   path separator) and the type is registered
   (`spinta/core/config.py:check_if_manifest_valid()`), the type is used
   as given, e.g. `csv:manifest.csv` or `internal:postgresql:///db`;
#. otherwise the type is detected by
   `spinta/core/config.py:parse_manifest_path()` →
   `spinta/manifests/helpers.py:detect_manifest_from_path()`, which asks
   every registered manifest class via its `detect_from_path()` static
   method (e.g. `.csv` → `CsvManifest`, `.txt` → `AsciiManifest`, a DSN
   with a `_manifest` table → `InternalSQLManifest`).

`ManifestPath` objects are then used by:

- `spinta/core/config.py:configure_rc()` — builds a temporary
  `RawConfig` fork for CLI commands and tests, turning `ManifestPath`
  arguments into `manifests.<name>` config entries (and `sync` chains
  for `inline` manifests);
- CLI commands that accept manifest arguments: `spinta copy`,
  `spinta inspect`, `spinta export`, `spinta import`, `spinta pull`,
  `spinta keymap`, `spinta config` (`spinta/cli/*.py`);
- test helpers (`spinta/testing/manifest.py`).

The `Manifest.path` attribute (on the loaded manifest) is set later by
the `configure` command of each manifest type from the
`manifests.<name>` config section — `ManifestPath` and `Manifest.path`
are therefore similar but separate things: the former carries the CLI
intent, the latter the configured location.

External specifications
***********************

- RML (https://rml.io/specs/rml/, local extract `.tmp/sources/rml/
  RML.md`) — `ManifestPath` plays the role of RML's *logical source*
  (`rml:source` plus `rml:referenceFormulation`): a pointer to an input
  plus its type, from which records are read. Spinta's `type` field
  corresponds to the reference formulation, `path`/`file` to the source
  location.
- DSA (https://dsa.readthedocs.io/, local extract `.tmp/sources/dsa/
  DSA.md`) — DSA requires the tabular manifest to be provided as CSV per
  RFC 4180 for automated processing; the `.csv`/`.txt`/`.xlsx`
  `detect_from_path()` rules implement exactly that file-format
  convention.
- OpenAPI (https://spec.openapis.org/oas/latest.html, local extract
  `.tmp/sources/openapi/OpenAPI.md`) — the `openapi+file://` URI scheme
  handled by `OpenAPIManifest.detect_from_path()` is an example of
  typing a location by its URI scheme, similar to how OpenAPI documents
  are referenced by URL in tooling.

See also
********

- :doc:`manifest` — the `Manifest` component the path points to.
- :doc:`/contrib/architecture/manifests/index` — manifest types and `detect_from_path()`.
