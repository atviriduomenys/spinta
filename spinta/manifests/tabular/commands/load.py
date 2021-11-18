import logging

from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.tabular.components import AsciiManifest
from spinta.manifests.tabular.components import CsvManifest
from spinta.manifests.tabular.components import TabularManifest
from spinta.manifests.tabular.helpers import read_tabular_manifest

log = logging.getLogger(__name__)


@commands.load.register(Context, TabularManifest)
def load(
    context: Context,
    manifest: TabularManifest,
    *,
    into: Manifest = None,
    freezed: bool = True,
    rename_duplicates: bool = False,
    load_internal: bool = True,
):
    assert freezed, (
        "TabularManifest does not have unfreezed version of manifest."
    )

    if load_internal:
        target = into or manifest
        if '_schema' not in target.models:
            store = context.get('store')
            commands.load(context, store.internal, into=target)

    file = None
    if isinstance(manifest, (CsvManifest, AsciiManifest)):
        file = manifest.file

    # There are cases, when we don't want to load any manifest.
    # For example, we want to start with an empty manifest, and then
    # generate it from external resources.
    if manifest.path is None and file is None:
        return

    if into:
        log.info(
            'Loading freezed manifest %r into %r from %s.',
            manifest.name,
            into.name,
            manifest.path,
        )
        schemas = read_tabular_manifest(
            manifest.format,
            path=manifest.path,
            file=file,
            rename_duplicates=rename_duplicates,
        )
        load_manifest_nodes(context, into, schemas, source=manifest)
    else:
        log.info(
            'Loading freezed manifest %r from %s.',
            manifest.name,
            manifest.path,
        )
        schemas = read_tabular_manifest(
            manifest.format,
            path=manifest.path,
            file=file,
            rename_duplicates=rename_duplicates,
        )
        load_manifest_nodes(context, manifest, schemas)

    for source in manifest.sync:
        commands.load(
            context, source,
            into=into or manifest,
            freezed=freezed,
            rename_duplicates=rename_duplicates,
            load_internal=load_internal,
        )
