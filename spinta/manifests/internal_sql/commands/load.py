import logging

from spinta import commands
from spinta.components import Context, Namespace
from spinta.manifests.internal_sql.components import InternalSQLManifest
from spinta.manifests.components import Manifest
from spinta.manifests.internal_sql.helpers import read_initial_schema, load_internal_manifest_nodes

log = logging.getLogger(__name__)


@commands.load_for_request.register(Context, InternalSQLManifest)
def load_for_request(context: Context, manifest: InternalSQLManifest):
    context.attach('transaction.manifest', manifest.transaction)
    schemas = read_initial_schema(context, manifest)
    load_internal_manifest_nodes(context, manifest, schemas)
    load_initial_empty_ns(context, manifest)

    if not commands.has_model(context, manifest, '_schema'):
        store = context.get('store')
        commands.load(context, store.internal, into=manifest)

    for source in manifest.sync:
        commands.load(
            context, source,
            into=manifest,
        )

    commands.link(context, manifest)


def load_initial_empty_ns(context: Context, manifest: InternalSQLManifest):
    ns = Namespace()
    data = {
        'type': 'ns',
        'name': '',
        'title': '',
        'description': '',
    }
    commands.load(context, ns, data, manifest)
    ns.generated = True


@commands.load.register(Context, InternalSQLManifest)
def load(
    context: Context,
    manifest: InternalSQLManifest,
    *,
    into: Manifest = None,
    freezed: bool = True,
    rename_duplicates: bool = False,
    load_internal: bool = True,
):
    pass
    # assert freezed, (
    #     "InternalSQLManifest does not have unfreezed version of manifest."
    # )
    #
    # if load_internal:
    #     target = into or manifest
    #     if not commands.has_model(context, target, '_schema'):
    #         store = context.get('store')
    #         commands.load(context, store.internal, into=target)

    #schemas = read_schema(manifest.path)

    # if into:
    #     log.info(
    #         'Loading freezed manifest %r into %r from %s.',
    #         manifest.name,
    #         into.name,
    #         manifest.path,
    #     )
    #     load_manifest_nodes(context, into, schemas, source=manifest)
    # else:
    #     log.info(
    #         'Loading freezed manifest %r from %s.',
    #         manifest.name,
    #         manifest.path,
    #     )
    #     load_manifest_nodes(context, manifest, schemas)

    # for source in manifest.sync:
    #     commands.load(
    #         context, source,
    #         into=into or manifest,
    #         freezed=freezed,
    #         rename_duplicates=rename_duplicates,
    #         load_internal=load_internal,
    #     )
