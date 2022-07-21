from spinta import commands
from spinta.core.ufuncs import Expr
from spinta.components import Context
from spinta.components import Model
from spinta.backends.memory.components import Memory


@commands.getall.register(Context, Model, Memory)
def getall(
    context: Context,
    model: Model,
    backend: Memory,
    *,
    query: Expr = None,
):
    return backend.db[model.name].values()
