import logging

from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.sql.components import SqlManifest
from spinta.manifests.sql.helpers import read_schema

log = logging.getLogger(__name__)


@commands.load.register(Context, SqlManifest)
def load(
    context: Context,
    manifest: SqlManifest,
    *,
    into: Manifest = None,
    freezed: bool = True,
    rename_duplicates: bool = False,
    load_internal: bool = True,
    full_load=False
):
    assert freezed, (
        "SqlManifest does not have unfreezed version of manifest."
    )

    if load_internal:
        target = into or manifest
        if not commands.has_model(context, target, '_schema'):
            store = context.get('store')
            commands.load(context, store.internal, into=target, full_load=full_load)

    dataset_name = context.get("rc").get("given_dataset_name")
    schemas = read_schema(context, manifest.path, manifest.prepare, dataset_name)

    if into:
        log.info(
            'Loading freezed manifest %r into %r from %s.',
            manifest.name,
            into.name,
            manifest.path,
        )
        load_manifest_nodes(context, into, schemas, source=manifest)
    else:
        log.info(
            'Loading freezed manifest %r from %s.',
            manifest.name,
            manifest.path,
        )
        load_manifest_nodes(context, manifest, schemas)

    for source in manifest.sync:
        commands.load(
            context, source,
            into=into or manifest,
            freezed=freezed,
            rename_duplicates=rename_duplicates,
            load_internal=load_internal,
            full_load=full_load
        )
