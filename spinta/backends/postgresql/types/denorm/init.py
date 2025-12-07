from spinta import commands
from spinta.backends.postgresql.helpers.type import validate_type_assignment
from spinta.components import Context
from spinta.types.datatype import Denorm
from spinta.backends.postgresql.components import PostgreSQL


@commands.prepare.register(Context, PostgreSQL, Denorm)
def prepare(context: Context, backend: PostgreSQL, dtype: Denorm, **kwargs):
    validate_type_assignment(context, backend, dtype)
    return
