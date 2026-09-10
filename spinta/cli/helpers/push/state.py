import datetime
import itertools
import json
from typing import Iterable, Iterator, List

import sqlalchemy as sa

from spinta import commands, spyna
from spinta.cli.helpers.push import prepare_data_for_push_state
from spinta.cli.helpers.push.components import PUSH_STATE_PATH, PushRow, PushState, Saved
from spinta.cli.helpers.push.utils import get_data_checksum
from spinta.cli.helpers.script.components import ScriptTag, ScriptTarget
from spinta.cli.helpers.script.helpers import sort_scripts_by_required
from spinta.cli.helpers.upgrade.registry import upgrade_script_registry
from spinta.components import Context, Model, pagination_enabled
from spinta.exceptions import PushStateMigrationRequired
from spinta.utils.json import fix_data_for_json
from spinta.utils.sqlite import migrate_table


def init_push_state(
    context: Context,
    dburi: str,
    models: List[Model],
) -> PushState:
    state = PushState(dburi)
    if not context.has(PUSH_STATE_PATH):
        context.set(PUSH_STATE_PATH, dburi)

    with state:
        is_fresh = is_fresh_database(context, state)
        # Initialize missing metadata tables
        state.create_all_metatables()

        # Create all missing model tables
        for model in models:
            state.get_table(name=model.name, model=model)

        if is_fresh:
            mark_migrations(push_state=state)
        else:
            validate_migrations(context, state)

            # Legacy self-healing destructive migrations (fixes issues with changed pagination columns)
            inspector = sa.inspect(state.engine)
            for model in models:
                expected_table = state.get_table(name=model.name, model=model)
                migrate_table(
                    state.engine,
                    state.metadata,
                    inspector,
                    expected_table,
                    renames={
                        "rev": "checksum",
                    },
                )
    return state


def is_fresh_database(context: Context, push_state: PushState) -> bool:
    insp = sa.inspect(push_state.engine)
    tables = insp.get_table_names()

    if not len(tables):
        return True

    for metatable_name in push_state.metatable_templates.keys():
        if metatable_name in tables:
            return False

    tables = [table for table in tables if not table.startswith("_")]
    manifest = context.get("store").manifest
    for table in tables:
        if commands.has_model(context, manifest, table):
            return False

    return True


def mark_migrations(push_state: PushState):
    # Mark all migration scripts as already executed
    migration_scripts = upgrade_script_registry.get_all(
        targets={ScriptTarget.PUSH_STATE_DB.value}, tags={ScriptTag.DB_MIGRATION.value}
    )
    filtered = sort_scripts_by_required(migration_scripts)
    for script in filtered.values():
        push_state.mark_migration(script.name)


def validate_migrations(context: Context, push_state: PushState):
    config = context.get("config")
    if config.upgrade_mode:
        return

    migration_scripts = upgrade_script_registry.get_all(
        targets={ScriptTarget.PUSH_STATE_DB.value}, tags={ScriptTag.DB_MIGRATION.value}
    )
    filtered = sort_scripts_by_required(migration_scripts)
    for script in filtered.values():
        if script.check(context):
            raise PushStateMigrationRequired(
                dsn=push_state.dsn, migration=script.name, path=push_state.engine.url.database
            )


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
