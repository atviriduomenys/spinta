from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.manifests.backend.components import BackendManifest


@commands.load.register(Context, BackendManifest)
def load(
    context: Context,
    manifest: BackendManifest,
    *,
    into: Manifest = None,
    rename_duplicates: bool = False,
    load_internal: bool = True,
    full_load=False,
):
    commands.load(context, manifest, manifest.backend, into=into)
