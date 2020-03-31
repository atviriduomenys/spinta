from types import AsyncGeneratorType

import sqlalchemy as sa

from starlette.requests import Request

from spinta import commands
from spinta.renderer import render
from spinta.utils.json import fix_data_for_json
from spinta.components import Context, Action, UrlParams, Model, Property
from spinta.backends.postgresql.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.sqlalchemy import utcnow


@commands.create_changelog_entry.register(Context, (Model, Property), PostgreSQL)
async def create_changelog_entry(
    context: Context,
    node: (Model, Property),
    backend: PostgreSQL,
    *,
    dstream: AsyncGeneratorType,
) -> None:
    transaction = context.get('transaction')
    connection = transaction.connection
    table = backend.get_table(node, TableType.CHANGELOG)
    async for data in dstream:
        qry = table.insert().values(
            _txn=transaction.id,
            datetime=utcnow(),
            action=Action.INSERT.value,
        )
        connection.execute(qry, [{
            '_rid': data.saved['_id'] if data.saved else data.patch['_id'],
            '_revision': data.patch['_revision'] if data.patch else data.saved['_revision'],
            '_txn': transaction.id,
            'datetime': utcnow(),
            'action': data.action.value,
            'data': fix_data_for_json({
                k: v for k, v in data.patch.items() if not k.startswith('_')
            }),
        }])
        yield data


@commands.changes.register()
async def changes(
    context: Context,
    request: Request,
    model: Model,
    backend: PostgreSQL,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, model)
    data = changes(context, model, backend, id_=params.pk, limit=params.limit, offset=params.offset)
    data = (
        {
            **row,
            '_created': row['_created'].isoformat(),
        }
        for row in data
    )
    return render(context, request, model, params, data, action=action)


@changes.register()
def changes(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    id_: str = None,
    limit: int = 100,
    offset: int = -10,
):
    connection = context.get('transaction').connection
    table = backend.get_table(model, TableType.CHANGELOG)

    qry = sa.select([table]).order_by(table.c._id)
    qry = _changes_id(table, qry, id_)
    qry = _changes_offset(table, qry, offset)
    qry = _changes_limit(qry, limit)

    result = connection.execute(qry)
    for row in result:
        yield {
            '_id': row[table.c._id],
            '_revision': row[table.c._revision],
            '_txn': row[table.c._txn],
            '_rid': row[table.c._rid],
            '_created': row[table.c.datetime],
            '_op': row[table.c.action],
            **dict(row[table.c.data]),
        }


def _changes_id(table, qry, id_):
    if id_:
        return qry.where(table.c._rid == id_)
    else:
        return qry


def _changes_offset(table, qry, offset):
    if offset:
        if offset > 0:
            offset = offset
        else:
            offset = (
                qry.with_only_columns([
                    sa.func.max(table.c.change) - abs(offset),
                ]).
                order_by(None).alias()
            )
        return qry.where(table.c.change > offset)
    else:
        return qry


def _changes_limit(qry, limit):
    if limit:
        return qry.limit(limit)
    else:
        return qry
