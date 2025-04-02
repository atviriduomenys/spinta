from typing import Dict, Any

from spinta import commands
from spinta.components import Context
from spinta.backends.memory.components import Memory


@commands.load.register(Context, Memory, dict)
def load(context: Context, backend: Memory, config: Dict[str, Any]):
    if 'dsn' in config:
        backend.dsn = config['dsn']
    backend.data = {}
