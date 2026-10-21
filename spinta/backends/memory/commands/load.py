from typing import Any

from spinta import commands
from spinta.backends.memory.components import Memory
from spinta.components import Context


@commands.load.register(Context, Memory, dict)
def load(context: Context, backend: Memory, config: dict[str, Any]):
    backend.data = {}
