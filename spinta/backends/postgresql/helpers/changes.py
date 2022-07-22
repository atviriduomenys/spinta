from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa

from sqlalchemy.dialects.postgresql import JSONB, BIGINT

from spinta import commands
from spinta.components import Context, Model
from spinta.backends.constants import TableType
from spinta.backends.helpers import get_table_name
from spinta.backends.postgresql.helpers import get_pg_name

if TYPE_CHECKING:
    from spinta.backends.postgresql.components import PostgreSQL


def get_changes_table(context: Context, backend: PostgreSQL, model: Model):
    table_name = get_pg_name(get_table_name(model, TableType.CHANGELOG))
    pkey_type = commands.get_primary_key_type(context, backend)
    table = sa.Table(
        table_name, backend.schema,
        # XXX: This will not work with multi master setup. Consider changing it
        #      to UUID or something like that.
        #
        #      `change` should be monotonically incrementing, in order to
        #      have that, we could always create new `change_id`, by querying,
        #      previous `change_id` and increment it by one. This will create
        #      duplicates, but we simply know, that these changes happened at at
        #      the same time. So that's probably OK.
        sa.Column('_id', BIGINT, primary_key=True),
        sa.Column('_revision', sa.String),
        sa.Column('_txn', pkey_type, index=True),
        sa.Column('_rid', pkey_type),  # reference to main table
        sa.Column('datetime', sa.DateTime),
        # FIXME: Change `action` to `_op` for consistency.
        sa.Column('action', sa.String(8)),  # insert, update, delete
        sa.Column('data', JSONB),
    )
    return table
