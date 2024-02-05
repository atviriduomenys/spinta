from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Denorm
from spinta.backends.postgresql.components import PostgreSQL


@commands.prepare.register(Context, PostgreSQL, Denorm)
def prepare(context: Context, backend: PostgreSQL, dtype: Denorm, **kwargs):
    return
