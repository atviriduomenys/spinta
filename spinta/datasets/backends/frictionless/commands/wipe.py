from spinta import commands
from spinta.components import Context
from spinta.components import Model
from spinta.datasets.backends.frictionless.components import FrictionlessBackend


@commands.wipe.register(Context, Model, FrictionlessBackend)
def wipe(context: Context, model: Model, backend: FrictionlessBackend):
    raise NotImplementedError
