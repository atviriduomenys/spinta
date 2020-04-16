from spinta import commands
from spinta.components import Context, Property, DataItem
from spinta.types.datatype import DataType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.validate import pg_check_unique_constraint


@commands.check_unique_constraint.register(Context, DataItem, DataType, Property, PostgreSQL, object)
def check_unique_constraint(
    context: Context,
    data: DataItem,
    dtype: DataType,
    prop: Property,
    backend: PostgreSQL,
    value: object,
):
    pg_check_unique_constraint(context, backend, prop, prop.name, data, value)
