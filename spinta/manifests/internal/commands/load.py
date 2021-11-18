import logging

from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.internal.components import InternalManifest
from spinta.manifests.yaml.helpers import read_manifest_schemas
from spinta.manifests.yaml.helpers import read_freezed_manifest_schemas

log = logging.getLogger(__name__)


@commands.load.register(Context, InternalManifest)
def load(
    context: Context,
    manifest: InternalManifest,
    *,
    into: Manifest = None,
    freezed: bool = True,
    rename_duplicates: bool = False,
    load_internal: bool = True,
):
    if freezed:
        if into:
            log.info(
                'Loading freezed manifest %r into %r from %s.',
                manifest.name,
                into.name,
                manifest.path,
            )
        else:
            log.info(
                'Loading freezed manifest %r from %s.',
                manifest.name,
                manifest.path,
            )
        schemas = read_freezed_manifest_schemas(manifest)
    else:
        if into:
            log.info(
                'Loading manifest %r into %r from %s.',
                manifest.name,
                into.name,
                manifest.path,
            )
        else:
            log.info(
                'Loading manifest %r from %s.',
                manifest.name,
                manifest.path,
            )
        schemas = read_manifest_schemas(manifest)

    if into:
        load_manifest_nodes(context, into, schemas, source=manifest)
    else:
        load_manifest_nodes(context, manifest, schemas)
