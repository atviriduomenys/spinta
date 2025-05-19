import logging
from typing import overload

import sqlalchemy as sa

from spinta import commands
from spinta.backends.postgresql.helpers.name import get_pg_table_name
from spinta.components import Context, Model
from spinta.types.datatype import DataType
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.helpers import get_pg_sequence_name, get_pg_name

log = logging.getLogger(__name__)


@overload
@commands.wipe.register(Context, Model, PostgreSQL)
def wipe(context: Context, model: Model, backend: PostgreSQL):
    table = backend.get_table(model, fail=False)
    if table is None:
        log.warning(
            f"Could not wipe {model.name}, because model is not fully "
            "initialized."
        )
        # Model backend might not be prepared, this is especially true for
        # tests. So if backend is not yet prepared, just skip this model.
        return

    for prop in model.properties.values():
        commands.wipe(context, prop.dtype, backend)

    connection = context.get('transaction').connection
    insp = sa.inspect(backend.engine)
    # Delete redirect table
    redirect_table_name = get_pg_table_name(model.name, TableType.REDIRECT)
    if insp.has_table(redirect_table_name):
        table = backend.get_table(model, TableType.REDIRECT)
        connection.execute(table.delete())

    # Delete changelog table
    changelog_table_name = get_pg_table_name(model.name, TableType.CHANGELOG)
    if insp.has_table(changelog_table_name):
        table = backend.get_table(model, TableType.CHANGELOG)
        connection.execute(table.delete())

        # Reset changelog table sequence
        table_name = get_table_name(model, TableType.CHANGELOG)
        seqname = get_pg_sequence_name(get_pg_name(table_name))
        connection.execute(f'ALTER SEQUENCE "{seqname}" RESTART')

    # Delete data table
    table = backend.get_table(model)
    connection.execute(table.delete())


@overload
@commands.wipe.register(Context, DataType, PostgreSQL)
def wipe(context: Context, dtype: DataType, backend: PostgreSQL):
    same_backend = backend.name == dtype.backend.name

    if not same_backend:
        commands.wipe(context, dtype, dtype.backend)
