from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa

from sqlalchemy.dialects.postgresql import JSONB, BIGINT

from spinta import commands
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.helpers import get_pg_name
from spinta.backends.postgresql.helpers.name import get_pg_column_name
from spinta.components import Context, Model
from spinta.backends.constants import TableType

if TYPE_CHECKING:
    from spinta.backends.postgresql.components import PostgreSQL


def get_changes_table(context: Context, backend: PostgreSQL, model: Model):
    table_name = get_table_name(model, TableType.CHANGELOG)
    pkey_type = commands.get_primary_key_type(context, backend)
    table = sa.Table(
        get_pg_name(table_name),
        backend.schema,
        # XXX: This will not work with multi master setup. Consider changing it
        #      to UUID or something like that.
        #
        #      `change` should be monotonically incrementing, in order to
        #      have that, we could always create new `change_id`, by querying,
        #      previous `change_id` and increment it by one. This will create
        #      duplicates, but we simply know, that these changes happened at at
        #      the same time. So that's probably OK.
        sa.Column(get_pg_column_name("_id"), BIGINT, primary_key=True, comment="_id"),
        sa.Column(get_pg_column_name("_revision"), sa.String, comment="_revision"),
        sa.Column(get_pg_column_name("_txn"), pkey_type, index=True, comment="_txn"),
        sa.Column(get_pg_column_name("_rid"), pkey_type, comment="_rid"),  # reference to main table
        sa.Column(get_pg_column_name("datetime"), sa.DateTime, comment="datetime"),
        # FIXME: Change `action` to `_op` for consistency.
        sa.Column(get_pg_column_name("action"), sa.String(8), comment="action"),  # insert, update, delete
        sa.Column(get_pg_column_name("data"), JSONB, comment="data"),
        comment=table_name,
    )
    return table
