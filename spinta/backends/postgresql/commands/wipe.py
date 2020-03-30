from spinta import commands
from spinta.components import Context, Model
from spinta.types.datatype import Array, DataType, Object
from spinta.backends.postgresql.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL


@commands.wipe.register()
def wipe(context: Context, model: Model, backend: PostgreSQL):
    table = backend.get_table(model, fail=False)
    if table is None:
        # Model backend might not be prepared, this is especially true for
        # tests. So if backend is not yet prepared, just skipt this model.
        return

    for prop in model.properties.values():
        wipe(context, prop.dtype, backend)

    connection = context.get('transaction').connection

    table = backend.get_table(model, TableType.CHANGELOG)
    connection.execute(table.delete())

    table = backend.get_table(model)
    connection.execute(table.delete())


@commands.wipe.register()
def wipe(context: Context, dtype: DataType, backend: PostgreSQL):
    pass


@commands.wipe.register()
def wipe(context: Context, dtype: Object, backend: PostgreSQL):
    for prop in dtype.properties.values():
        wipe(context, prop.dtype, backend)


@commands.wipe.register()
def wipe(context: Context, dtype: Array, backend: PostgreSQL):
    wipe(context, dtype.items.dtype, backend)
    table = backend.get_table(dtype.prop, TableType.LIST)
    connection = context.get('transaction').connection
    connection.execute(table.delete())
