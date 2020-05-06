from spinta import commands
from spinta.components import Context
from spinta.manifests.backend.components import BackendManifest


@commands.freeze.register(Context, BackendManifest)
def freeze(context: Context, current: BackendManifest):
    # Backend manifest delegates freezing to its sync manifests.
    for source in current.sync:
        commands.freeze(context, source)
