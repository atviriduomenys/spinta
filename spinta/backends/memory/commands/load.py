from typing import Any, Dict

from spinta import commands
from spinta.backends.memory.components import Memory
from spinta.components import Context


@commands.load.register(Context, Memory, dict)
def load(context: Context, backend: Memory, config: Dict[str, Any]):
    backend.data = {}
