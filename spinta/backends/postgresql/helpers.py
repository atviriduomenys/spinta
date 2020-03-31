from __future__ import annotations

from typing import TYPE_CHECKING, Union

import hashlib

import sqlalchemy as sa

from sqlalchemy.dialects.postgresql import JSONB, BIGINT

from spinta import commands
from spinta.components import Context, Model, Property
from spinta.backends.postgresql.constants import TableType
from spinta.backends.postgresql.constants import NAMEDATALEN

if TYPE_CHECKING:
    from spinta.backends.postgresql.components import PostgreSQL


def get_table_name(
    node: Union[Model, Property],
    ttype: TableType = TableType.MAIN,
) -> str:
    if isinstance(node, Model):
        model = node
    else:
        model = node.model
    if ttype in (TableType.LIST, TableType.FILE):
        name = model.model_type() + ttype.value + '/' + node.place
    else:
        name = model.model_type() + ttype.value
    return name


def get_column_name(prop: Property):
    if prop.list:
        if prop.place == prop.list.place:
            return prop.list.name
        else:
            return prop.place[len(prop.list.place) + 1:]
    else:
        return prop.place


def get_pg_name(name: str) -> str:
    if len(name) > NAMEDATALEN:
        hs = 8
        h = hashlib.sha1(name.encode()).hexdigest()[:hs]
        i = int(NAMEDATALEN / 100 * 60)
        j = NAMEDATALEN - i - hs - 2
        name = name[:i] + '_' + h + '_' + name[-j:]
    return name


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


def flat_dicts_to_nested(value):
    res = {}
    for k, v in dict(value).items():
        names = k.split('.')
        vref = res
        for name in names[:-1]:
            if name not in vref:
                vref[name] = {}
            vref = vref[name]
        vref[names[-1]] = v
    return res
