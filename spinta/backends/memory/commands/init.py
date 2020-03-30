from spinta import commands
from spinta.components import Context
from spinta.components import Manifest
from spinta.backends.memory.components import Memory


@commands.prepare.register()
def prepare(context: Context, backend: Memory, manifest: Manifest):
    pass
