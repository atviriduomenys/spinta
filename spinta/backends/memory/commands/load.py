from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.backends.memory.components import Memory


@commands.load.register(Context, Memory, RawConfig)
def load(context: Context, backend: Memory, rc: RawConfig):
    backend.data = {}
