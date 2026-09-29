from typing import Iterator

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_identifier
from spinta.backends.memory.components import Memory
from spinta.components import Context, Model
from spinta.core.ufuncs import Expr
from spinta.typing import ObjectData


@commands.getall.register(Context, Model, Memory)
def getall(context: Context, model: Model, db: Memory, *, query: Expr = None, **kwargs) -> Iterator[ObjectData]:
    table = get_table_identifier(model)
    return db.data[table.logical_qualified_name].values()


@commands.getone.register(Context, Model, Memory)
def getone(
    context: Context,
    model: Model,
    db: Memory,
    *,
    id_: str,
) -> ObjectData:
    table = get_table_identifier(model)
    return db.data[table.logical_qualified_name][id_]


@commands.changes.register(Context, Model, Memory)
def changes(
    context: Context,
    model: Model,
    db: Memory,
    *,
    id_: str = None,
    limit: int = 100,
    offset: int = -10,
):
    table = get_table_identifier(model, TableType.CHANGELOG)
    return db.data[table.logical_qualified_name].values()
