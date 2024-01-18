from spinta.backends.nobackend.components import NoBackend
from spinta.exceptions import BackendNotGiven
from spinta import commands
from spinta.core.ufuncs import Expr
from spinta.components import Context, UrlParams
from spinta.components import Model


@commands.getall.register(Context, Model, NoBackend)
def getall(
    context: Context,
    model: Model,
    backend: NoBackend,
    *,
    query: Expr = None,
    **kwargs
):
    raise BackendNotGiven(model)


@commands.getone.register(Context, Model, NoBackend)
def getone(
    context: Context,
    model: Model,
    db: NoBackend,
    *,
    id_: str,
):
    raise BackendNotGiven(model)


@commands.changes.register(Context, Model, NoBackend)
def changes(
    context: Context,
    model: Model,
    db: NoBackend,
    *,
    id_: str = None,
    limit: int = 100,
    offset: int = -10,
):
    raise BackendNotGiven(model)
