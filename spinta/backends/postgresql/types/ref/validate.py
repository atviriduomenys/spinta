from spinta import commands
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers.validate import pg_check_unique_constraint
from spinta.components import Context, DataItem, Property
from spinta.types.datatype import Ref


@commands.check_unique_constraint.register(Context, DataItem, Ref, Property, PostgreSQL, str)
def check_unique_constraint(
    context: Context,
    data: DataItem,
    dtype: Ref,
    prop: Property,
    backend: PostgreSQL,
    value: str,
):
    pg_check_unique_constraint(context, backend, prop, prop.name + "._id", data, value)


@commands.check_unique_constraint.register(Context, DataItem, Ref, Property, PostgreSQL, dict)
def check_unique_constraint(
    context: Context,
    data: DataItem,
    dtype: Ref,
    prop: Property,
    backend: PostgreSQL,
    value: dict,
):
    pg_check_unique_constraint(context, backend, prop, prop.name + "._id", data, value.get("_id"))
