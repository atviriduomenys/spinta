import logging

from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.backend.components import BackendManifest
from spinta.manifests.backend.helpers import read_manifest_schemas

log = logging.getLogger(__name__)


@commands.load.register(Context, BackendManifest)
def load(
    context: Context,
    manifest: BackendManifest,
    *,
    into: Manifest = None,
    freezed: bool = True,
):
    assert freezed, (
        "BackendManifest does not have unfreezed version of manifest."
    )

    log.info(
        'Loading manifest %r from %s backend.',
        manifest.name,
        manifest.backend,
    )

    schemas = read_manifest_schemas(context, manifest)
    target = into or manifest
    load_manifest_nodes(context, target, schemas)
