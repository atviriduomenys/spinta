import datetime
import json

import sqlalchemy as sa

from spinta import spyna
from spinta.cli.helpers.push import prepare_data_for_push_state
from spinta.cli.helpers.push.components import PUSH_SESSION_ID, PushOperation, PushRow, PushRows
from spinta.components import Context, Model, pagination_enabled
from spinta.utils.json import fix_data_for_json
from spinta.utils.sqlite import migrate_table


def init_push_state(
    dburi: str,
    models: list[Model],
) -> tuple[sa.engine.Engine, sa.MetaData]:
    engine = sa.create_engine(dburi)
    metadata = sa.MetaData(engine)
    inspector = sa.inspect(engine)

    page_table = sa.Table(
        "_page",
        metadata,
        sa.Column("model", sa.Text, primary_key=True),
        sa.Column("property", sa.Text),
        sa.Column("value", sa.Text),
    )
    page_table.create(checkfirst=True)

    types = {
        "string": sa.Text,
        "date": sa.Date,
        "datetime": sa.DateTime,
        "time": sa.Time,
        "integer": sa.Integer,
        "number": sa.Numeric,
    }

    for model in models:
        pagination_cols = []
        if pagination_enabled(model):
            for prop in model.page.keys.values():
                _type = types.get(prop.dtype.name, sa.Text)
                pagination_cols.append(sa.Column(f"page.{prop.name}", _type, index=True))

        table = sa.Table(
            model.name,
            metadata,
            sa.Column("id", sa.Unicode, primary_key=True),
            sa.Column("checksum", sa.Unicode),
            sa.Column("revision", sa.Unicode),
            sa.Column("pushed", sa.DateTime),
            sa.Column("error", sa.Boolean),
            sa.Column("data", sa.Text),
            sa.Column("session_id", sa.Unicode, index=True),
            *pagination_cols,
        )
        migrate_table(
            engine,
            metadata,
            inspector,
            table,
            renames={
                "rev": "checksum",
            },
        )

    return engine, metadata


def reset_pushed(
    context: Context,
    models: list[Model],
    metadata: sa.MetaData,
):
    conn = context.get("push.state.conn")
    for model in models:
        table = metadata.tables[model.name]

        # reset pushed so we could see which objects were deleted
        conn.execute(table.update().values(pushed=None))


def get_state_row(
    context: Context,
    model: Model,
    id_: str | None,
    metadata: sa.MetaData,
) -> dict | None:
    if id_ is None:
        return None

    conn = context.get("push.state.conn")
    table = metadata.tables[model.name]
    query = table.select().where(table.c.id == id_)
    return conn.execute(query).one_or_none()


def consume_unchanged_rows(
    context: Context,
    rows: PushRows,
    metadata: sa.MetaData,
    *,
    dry_run: bool = False,
) -> PushRows:
    conn = context.get("push.state.conn")
    session_id = context.get(PUSH_SESSION_ID)
    for row in rows:
        if row.op is not PushOperation.UNCHANGED:
            yield row
            continue

        table = metadata.tables[row.model.model_type()]
        row_id = row.data.get("_id", None)
        if row_id is None:
            continue

        if not dry_run:
            conn.execute(
                table.update()
                .where(table.c.id == row_id)
                .values(
                    session_id=session_id,
                    pushed=datetime.datetime.now(),
                )
            )


def save_push_state(
    context: Context,
    rows: PushRows,
    metadata: sa.MetaData,
) -> PushRows:
    conn = context.get("push.state.conn")
    page_table = metadata.tables["_page"]
    model_pagination_check = {}
    session_id = context.get(PUSH_SESSION_ID)
    for row in rows:
        table = metadata.tables[row.data["_type"]]
        if "_id" in row.data:
            _id = row.data["_id"]
        else:
            _id = spyna.parse(row.data["_where"])["args"][1]

        if row.op is PushOperation.DELETE and not row.error:
            conn.execute(table.delete().where(table.c.id == _id))
            yield row
            continue

        model_name = row.model.model_type()
        if model_name not in model_pagination_check:
            model_pagination_check[model_name] = pagination_enabled(row.model)

        if model_pagination_check[model_name] and "_page" in row.data:
            loaded = row.data["_page"]
            page = {prop.name: loaded[i] for i, prop in enumerate(row.model.page.keys.values())}
            page = prepare_data_for_push_state(context, row.model, page)
            save_page_values(conn=conn, table=page_table, model=row.model, page_data=page)
            page = {f"page.{key}": value for key, value in page.items()}
            row.data.pop("_page")
        else:
            page = {}

        if row.error:
            data = fix_data_for_json(row.data)
            data = json.dumps(data)
        else:
            data = None

        if row.saved:
            conn.execute(
                table.update()
                .where(table.c.id == _id)
                .values(
                    id=_id,
                    revision=row.data.get("_revision"),
                    checksum=row.checksum,
                    pushed=datetime.datetime.now(),
                    error=row.error,
                    data=data,
                    session_id=session_id,
                    **page,
                )
            )
        else:
            conn.execute(
                table.insert().values(
                    id=_id,
                    revision=row.data.get("_revision"),
                    checksum=row.checksum,
                    pushed=datetime.datetime.now(),
                    error=row.error,
                    data=data,
                    session_id=session_id,
                    **page,
                )
            )
        yield row


def save_page_values(
    conn: sa.engine.Connection,
    table: sa.Table,
    model: Model,
    page_data: dict,
):
    model_name = model.model_type()
    page_row = conn.execute(sa.select(table.c.model).where(table.c.model == model_name)).scalar()

    exists = page_row is not None
    page_values = {key: value for key, value in page_data.items() if value is not None}

    if not page_values:
        return

    page_values = fix_data_for_json(page_values)
    page_values = json.dumps(page_values)

    if exists:
        conn.execute(table.update().where((table.c.model == model_name)).values(value=page_values))
    else:
        conn.execute(
            table.insert().values(
                model=model.name, property=",".join([prop.name for prop in model.page.keys.values()]), value=page_values
            )
        )


def _get_model_type(row: PushRow) -> str:
    return row.data["_type"]
