import json

from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.simple.components import SimpleFileManifest
from spinta.manifests.simple.helpers import check_format, read_simple_rows
from spinta.manifests.simple.types import SimpleManifestError
from spinta.manifests.tabular.helpers import _read_tabular_manifest_rows


def _read_document(manifest: SimpleFileManifest) -> dict | None:
    """Parse the JSON of `file` (in memory) or `path` (on disk), or `None`."""
    if manifest.file is not None:
        text = manifest.file.read()
        source = manifest.path or "<memory>"
    elif manifest.path is not None:
        with open(manifest.path, encoding="utf-8") as f:
            text = f.read()
        source = manifest.path
    else:
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise SimpleManifestError(f"{source}: {e}") from e


@commands.load.register(Context, SimpleFileManifest)
def load(
    context: Context,
    manifest: SimpleFileManifest,
    *,
    into: Manifest = None,
    rename_duplicates: bool = False,
    load_internal: bool = True,
    full_load=False,
):
    if load_internal:
        target = into or manifest
        if not commands.has_model(context, target, "_schema"):
            store = context.get("store")
            commands.load(context, store.internal, into=target)

    document = _read_document(manifest)
    if document is None:
        return
    check_format(document)

    schemas = _read_tabular_manifest_rows(
        manifest.path,
        read_simple_rows(document),
        rename_duplicates=rename_duplicates,
    )
    if into:
        load_manifest_nodes(context, into, schemas, source=manifest)
    else:
        load_manifest_nodes(context, manifest, schemas)

    for source in manifest.sync:
        commands.load(
            context,
            source,
            into=into or manifest,
            rename_duplicates=rename_duplicates,
            load_internal=load_internal,
            full_load=full_load,
        )
