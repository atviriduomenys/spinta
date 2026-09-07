import datetime
import itertools
import json
from typing import Iterable, Iterator, List

import sqlalchemy as sa

from spinta import spyna
from spinta.cli.helpers.push import prepare_data_for_push_state
from spinta.cli.helpers.push.components import PushRow, PushState, Saved
from spinta.cli.helpers.push.utils import get_data_checksum
from spinta.components import Context, Model, pagination_enabled
from spinta.utils.json import fix_data_for_json


def init_push_state(
    dburi: str,
    models: List[Model],
) -> PushState:
    state = PushState(dburi)

    # Initialize metadata tables
    state.create_all_metatables()

    # Create all model tables
    for model in models:
        state.get_table(name=model.name, model=model)
    return state


def reset_pushed(
    context: Context,
    models: List[Model],
    push_state: PushState,
):
    conn = push_state.conn

    for model in models:
        table = push_state.get_table(model.name)

        # reset pushed so we could see which objects were deleted
        conn.execute(table.update().values(pushed=None))


def check_push_state(rows: Iterable[PushRow], push_state: PushState):
    conn = push_state.conn

    for model_type, group in itertools.groupby(rows, key=_get_model_type):
        saved_rows = {}
        if model_type:
            table = push_state.get_table(model_type)

            query = sa.select([table.c.id, table.c.revision, table.c.checksum])
            saved_rows = {
                state[table.c.id]: Saved(
                    state[table.c.revision],
                    state[table.c.checksum],
                )
                for state in conn.execute(query)
            }

        for row in group:
            if row.send and not row.error and row.op != "delete":
                _id = row.data["_id"]
                row.checksum = get_data_checksum(row.data, row.model)
                saved = saved_rows.get(_id)
                if saved is None:
                    row.op = "insert"
                    row.saved = False
                else:
                    row.op = "patch"
                    row.saved = True
                    row.data["_revision"] = saved.revision
                    if saved.checksum == row.checksum:
                        conn.execute(table.update().where(table.c.id == _id).values(pushed=datetime.datetime.now()))
                        # Skip if no changes, but only if not paginated, since pagination already skips those
                        if "_page" not in row.data:
                            continue

            yield row


def save_push_state(
    context: Context,
    rows: Iterable[PushRow],
    push_state: PushState,
) -> Iterator[PushRow]:
    conn = push_state.conn
    page_table = push_state.get_table(push_state.pagination_table_name)
    model_pagination_check = {}
    for row in rows:
        table = push_state.get_table(row.data["_type"])
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

        if row.error and row.op != "delete":
            data = fix_data_for_json(row.data)
            data = json.dumps(data)
        else:
            data = None

        if "_id" in row.data:
            _id = row.data["_id"]
        else:
            _id = spyna.parse(row.data["_where"])["args"][1]

        if row.op == "delete" and not row.error:
            conn.execute(table.delete().where(table.c.id == _id))
        elif row.saved:
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
                    **page,
                )
            )
        yield row


def save_page_values(conn: sa.engine.Connection, table: sa.Table, model: Model, page_data: dict):
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
