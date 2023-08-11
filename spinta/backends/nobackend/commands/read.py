from spinta.backends.helpers import get_table_name
from spinta.backends.nobackend.components import NoBackend
from spinta.exceptions import TableHasNoSource
from spinta import commands
from spinta.core.ufuncs import Expr
from spinta.components import Context
from spinta.components import Model


@commands.getall.register(Context, Model, NoBackend)
def getall(
    context: Context,
    model: Model,
    backend: NoBackend,
    *,
    query: Expr = None,
):
    table = get_table_name(model)
    raise TableHasNoSource(table)


@commands.getone.register(Context, Model, NoBackend)
def getone(
    context: Context,
    model: Model,
    db: NoBackend,
    *,
    id_: str,
):
    table = get_table_name(model)
    raise TableHasNoSource(table)


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
    table = get_table_name(model)
    raise TableHasNoSource(table)
