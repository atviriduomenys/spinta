from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.components import Context, Model

if TYPE_CHECKING:
    from spinta.backends.postgresql.components import PostgreSQL


def get_redirect_table(context: Context, backend: PostgreSQL, model: Model):
    table_name = get_pg_name(get_table_name(model, TableType.REDIRECT))
    pkey_type = commands.get_primary_key_type(context, backend)
    table = sa.Table(
        table_name,
        backend.schema,
        sa.Column("_id", pkey_type, primary_key=True),
        sa.Column("redirect", pkey_type, index=True),
    )
    return table


def remove_from_redirect(conn: sa.engine.Connection, table: sa.Table, pk: str):
    conn.execute(table.delete().where(table.columns["_id"] == pk))
