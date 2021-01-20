from typing import Any
from typing import Dict

from spinta import commands
from spinta.components import Context
from spinta.datasets.backends.frictionless.components import FrictionlessBackend


@commands.load.register(Context, FrictionlessBackend, dict)
def load(context: Context, backend: FrictionlessBackend, config: Dict[str, Any]):
    dsn = config['dsn']
