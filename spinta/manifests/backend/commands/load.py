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
    freezed: bool = True,
    rename_duplicates: bool = False,
    load_internal: bool = True,
):
    assert freezed, (
        "BackendManifest does not have unfreezed version of manifest."
    )
    commands.load(context, manifest, manifest.backend, into=into, freezed=freezed)
