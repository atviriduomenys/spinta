import sqlalchemy as sa

from spinta.utils.schema import NA
from spinta.components import Context, Action, Property
from spinta.components import Context, Property, Action, DataItem
from spinta.backends.postgresql.components import PostgreSQL
from spinta.exceptions import UniqueConstraint


def pg_check_unique_constraint(
    context: Context,
    backend: PostgreSQL,
    prop: Property,
    name: str,
    data: DataItem,
    value: str,
):
    table = backend.get_table(prop)

    if (
        data.action in (Action.UPDATE, Action.PATCH) or
        data.action == Action.UPSERT and data.saved is not NA
    ):
        if prop.name == '_id' and value == data.saved['_id']:
            return
        condition = sa.and_(
            table.c[name] == value,
            table.c._id != data.saved['_id'],
        )
    else:
        condition = table.c[name] == value
    connection = context.get('transaction').connection
    default = object()
    result = backend.get(connection, table.c[name], condition, default=default)
    if result is not default:
        raise UniqueConstraint(prop)
