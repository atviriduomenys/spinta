from types import AsyncGeneratorType
from typing import Optional

import sqlalchemy as sa

from spinta import commands
from spinta.backends.constants import TableType
from spinta.backends.postgresql.components import PostgreSQL
from spinta.backends.postgresql.sqlalchemy import utcnow
from spinta.components import Context, Model, Property, DataItem
from spinta.core.enums import Action
from spinta.utils.json import fix_data_for_json


@commands.create_changelog_entry.register(Context, (Model, Property), PostgreSQL)
async def create_changelog_entry(
    context: Context,
    node: (Model, Property),
    backend: PostgreSQL,
    *,
    dstream: AsyncGeneratorType,
) -> None:
    transaction = context.get("transaction")
    connection = transaction.connection
    table = backend.get_table(node, TableType.CHANGELOG)
    async for data in dstream:
        if not data.patch:
            yield data
            continue

        qry = table.insert().values(
            _txn=transaction.id,
            datetime=utcnow(),
            action=Action.INSERT.value,
        )
        filtered_data = _filter_data_by_action(data)
        connection.execute(
            qry,
            [
                {
                    "_rid": data.saved["_id"] if data.saved else data.patch["_id"],
                    "_revision": data.patch["_revision"] if data.patch else data.saved["_revision"],
                    "_txn": transaction.id,
                    "datetime": utcnow(),
                    "action": data.action.value,
                    "data": fix_data_for_json(filtered_data),
                }
            ],
        )
        yield data


@commands.changes.register(Context, Model, PostgreSQL)
def changes(
    context: Context,
    model: Model,
    backend: PostgreSQL,
    *,
    id_: str = None,
    limit: int = 100,
    offset: Optional[int] = -100,
):
    connection = context.get("transaction").connection
    table = backend.get_table(model, TableType.CHANGELOG)

    qry = sa.select([table]).order_by(table.c["_id"].asc())
    qry = _changes_id(table, qry, id_)
    qry = _changes_offset(table, qry, offset)
    qry = _changes_limit(qry, limit)

    result = connection.execute(qry)
    for row in result:
        data = dict(row[table.c["data"]] or {})
        if row[table.c["action"]] == "move":
            data = {"_same_as": data.get("_id")}
        yield {
            "_cid": row[table.c["_id"]],
            "_created": row[table.c["datetime"]],
            "_op": row[table.c["action"]],
            "_id": row[table.c["_rid"]],
            "_txn": row[table.c["_txn"]],
            "_revision": row[table.c["_revision"]],
            **data,
        }


def _changes_id(table, qry, id_):
    if id_:
        return qry.where(table.c["_rid"] == id_)
    else:
        return qry


def _changes_offset(table, qry, offset):
    if offset is not None:
        if offset >= 0:
            return qry.where(table.c["_id"] >= offset)
        else:
            offset = (
                qry.with_only_columns(
                    [
                        sa.func.max(table.c["_id"]) - abs(offset),
                    ]
                )
                .order_by(None)
                .scalar_subquery()
            )
            return qry.where(table.c["_id"] > offset)
    else:
        return qry


def _changes_limit(qry, limit):
    if limit:
        return qry.limit(limit)
    else:
        return qry


def _filter_data_by_action(data: DataItem) -> dict:
    if data.action == Action.MOVE:
        return {"_id": data.patch.get("_id")}

    return {k: v for k, v in data.patch.items() if not k.startswith("_")}
