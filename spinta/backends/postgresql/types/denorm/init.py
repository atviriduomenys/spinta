from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Denorm
from spinta.backends.postgresql.components import PostgreSQL


@commands.prepare.register(Context, PostgreSQL, Denorm)
def prepare(context: Context, backend: PostgreSQL, dtype: Denorm):
    rel_prop = dtype.rel_prop
    original_place = rel_prop.place
    rel_prop.place = dtype.prop.place
    column = commands.prepare(
        context,
        backend,
        rel_prop.dtype,
    )
    rel_prop.place = original_place
    return column
