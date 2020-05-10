import logging

from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.manifests.backend.components import BackendManifest
from spinta.backends.memory.components import Memory

log = logging.getLogger(__name__)


@commands.load.register(Context, BackendManifest, Memory)
def load(
    context: Context,
    manifest: BackendManifest,
    backend: Memory,
    *,
    into: Manifest = None,
    freezed: bool = True,
) -> None:
    for source in manifest.sync:
        commands.load(context, source, into=into or manifest, freezed=freezed)
