from spinta import commands
from spinta.components import Context
from spinta.manifests.backend.components import BackendManifest


@commands.configure.register(Context, BackendManifest)
def configure(context: Context, manifest: BackendManifest):
    pass
