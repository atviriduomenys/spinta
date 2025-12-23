from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_identifier
from spinta.backends.postgresql.helpers.name import get_pg_column_name
from spinta.components import Context, Model

if TYPE_CHECKING:
    from spinta.backends.postgresql.components import PostgreSQL


def get_redirect_table(context: Context, backend: PostgreSQL, model: Model):
    table_identifier = get_table_identifier(model, TableType.REDIRECT)
    pkey_type = commands.get_primary_key_type(context, backend)
    table = sa.Table(
        table_identifier.pg_table_name,
        backend.schema,
        sa.Column(get_pg_column_name("_id"), pkey_type, primary_key=True, comment="_id"),
        sa.Column(get_pg_column_name("redirect"), pkey_type, index=True, comment="redirect"),
        schema=table_identifier.pg_schema_name,
        comment=table_identifier.logical_qualified_name,
    )
    return table


def remove_from_redirect(conn: sa.engine.Connection, table: sa.Table, pk: str):
    conn.execute(table.delete().where(table.columns["_id"] == pk))
