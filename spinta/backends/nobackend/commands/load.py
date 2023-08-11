from typing import Dict, Any

from spinta import commands
from spinta.components import Context
from spinta.backends.nobackend.components import NoBackend


@commands.load.register(Context, NoBackend, dict)
def load(context: Context, backend: NoBackend, config: Dict[str, Any]):
    backend.data = {}
