from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest


@commands.inspect.register(Context, Manifest)
def inspect(context: Context, manifest: Manifest):
    for backend in manifest.backends.values():
        commands.inspect(context, manifest, backend)
