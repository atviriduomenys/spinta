from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.backends.memory.components import Memory


@commands.prepare.register(Context, Memory, Manifest)
def prepare(context: Context, backend: Memory, manifest: Manifest):
    for model in manifest.models.values():
        backend.add_model(model)
