from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Object
from spinta.backends.postgresql.components import PostgreSQL


@commands.prepare.register(Context, PostgreSQL, Object)
def prepare(context: Context, backend: PostgreSQL, dtype: Object):
    columns = []
    for prop in dtype.properties.values():
        if prop.name.startswith('_') and prop.name not in ('_revision',):
            continue
        column = commands.prepare(context, backend, prop)
        if isinstance(column, list):
            columns.extend(column)
        elif column is not None:
            columns.append(column)
    return columns
