from spinta import commands
from spinta.components import Context
from spinta.types.datatype import ExternalRef
from spinta.backends.postgresql.components import PostgreSQL


@commands.prepare.register(Context, PostgreSQL, ExternalRef)
def prepare(context: Context, backend: PostgreSQL, dtype: ExternalRef):
    columns = []
    for prop in dtype.refprops:
        original_place = prop.place
        prop.place = f"{dtype.prop.place}.{prop.place}"
        column = commands.prepare(
            context,
            backend,
            prop.dtype,
        )
        columns.append(column)
        prop.place = original_place
    return columns
