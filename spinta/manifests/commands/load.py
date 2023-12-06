from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest


@commands.load_for_request.register(Context, Manifest)
def load_for_request(context: Context, manifest: Manifest):
    pass


@commands.fully_initialize_manifest.register(Context, Manifest)
def fully_initialize_manifest(context: Context, manifest: Manifest):
    pass


@commands.create_request_manifest.register(Context, Manifest)
def create_request_manifest(context: Context, manifest: Manifest):
    return manifest
