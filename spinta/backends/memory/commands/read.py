from typing import Iterator

from spinta import commands
from spinta.core.ufuncs import Expr
from spinta.components import Context
from spinta.components import Model
from spinta.typing import ObjectData
from spinta.backends.memory.components import Memory
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name


@commands.getall.register(Context, Model, Memory)
def getall(
    context: Context,
    model: Model,
    db: Memory,
    *,
    query: Expr = None,
) -> Iterator[ObjectData]:
    table = get_table_name(model)
    return db.data[table].values()


@commands.getone.register(Context, Model, Memory)
def getone(
    context: Context,
    model: Model,
    db: Memory,
    *,
    id_: str,
) -> ObjectData:
    table = get_table_name(model)
    return db.data[table][id_]


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
    table = get_table_name(model, TableType.CHANGELOG)
    return db.data[table].values()
