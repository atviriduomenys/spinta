import sqlalchemy as sa

from spinta import commands
from spinta.utils.schema import NA
from spinta.components import Context, Action, Property
from spinta.types.datatype import DataType
from spinta.components import Context, Property, Action, DataItem
from spinta.backends.postgresql.components import PostgreSQL
from spinta.exceptions import UniqueConstraint


@commands.check_unique_constraint.register(Context, DataItem, DataType, Property, PostgreSQL, object)
def check_unique_constraint(
    context: Context,
    data: DataItem,
    dtype: DataType,
    prop: Property,
    backend: PostgreSQL,
    value: object,
):
    table = backend.get_table(prop)

    if (
        data.action in (Action.UPDATE, Action.PATCH) or
        data.action == Action.UPSERT and data.saved is not NA
    ):
        if prop.name == '_id' and value == data.saved['_id']:
            return
        condition = sa.and_(
            table.c[prop.name] == value,
            table.c._id != data.saved['_id'],
        )
    else:
        condition = table.c[prop.name] == value
    not_found = object()
    connection = context.get('transaction').connection
    result = backend.get(connection, table.c[prop.name], condition, default=not_found)
    if result is not not_found:
        raise UniqueConstraint(prop)
