import logging

from spinta import commands
from spinta.components import Context, Model
from spinta.types.datatype import DataType
from spinta.backends.postgresql.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL

log = logging.getLogger(__name__)


@commands.wipe.register(Context, Model, PostgreSQL)
def wipe(context: Context, model: Model, backend: PostgreSQL):
    table = backend.get_table(model, fail=False)
    if table is None:
        log.warning(
            f"Could not wipe {model.name}, because model is not fully "
            "initialized."
        )
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


@commands.wipe.register(Context, DataType, PostgreSQL)
def wipe(context: Context, dtype: DataType, backend: PostgreSQL):
    same_backend = backend.name == dtype.backend.name

    if not same_backend:
        wipe(context, dtype, dtype.backend)
