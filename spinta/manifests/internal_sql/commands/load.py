import logging

from spinta import commands
from spinta.components import Context, Namespace
from spinta.manifests.helpers import init_manifest, _configure_manifest, load_manifest_nodes
from spinta.manifests.internal_sql.components import InternalSQLManifest
from spinta.manifests.components import Manifest
from spinta.manifests.internal_sql.helpers import read_initial_schema, load_internal_manifest_nodes, read_schema

log = logging.getLogger(__name__)


@commands.create_request_manifest.register(Context, InternalSQLManifest)
def create_request_manifest(context: Context, manifest: InternalSQLManifest):
    old = manifest
    store = manifest.store
    manifest = old.__class__()
    rc = context.get('rc')
    init_manifest(context, manifest, old.name)
    _configure_manifest(
        context, rc, store, manifest,
        backend=store.manifest.backend.name if store.manifest.backend else None,
    )
    commands.load(context, manifest)
    commands.link(context, manifest)
    return manifest


@commands.load_for_request.register(Context, InternalSQLManifest)
def load_for_request(context: Context, manifest: InternalSQLManifest):
    context.attach('transaction.manifest', manifest.transaction)
    schemas = read_initial_schema(context, manifest)
    load_internal_manifest_nodes(context, manifest, schemas, link=True)


@commands.load.register(Context, InternalSQLManifest)
def load(
    context: Context,
    manifest: InternalSQLManifest,
    *,
    into: Manifest = None,
    freezed: bool = True,
    rename_duplicates: bool = False,
    load_internal: bool = True,
    full_load=False
):
    if load_internal:
        target = into or manifest
        if '_schema' not in target.get_objects()['model']:
            store = context.get('store')
            commands.load(context, store.internal, into=target, full_load=full_load)

    if full_load:
        schemas = read_schema(manifest.path)
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

