from typing import TYPE_CHECKING

import msgpack
import sqlalchemy as sa
from tqdm import tqdm

from spinta.cli.helpers.upgrade.components import Script
from spinta.cli.helpers.upgrade.scripts.keymaps.sqlalchemy.helpers import (
    requires_migration,
    apply_migration_to_outdated_keymaps,
    reset_keymap_increment,
)
from spinta.components import Context

if TYPE_CHECKING:
    from spinta.datasets.keymaps.sqlalchemy import SqlAlchemyKeyMap


def requires_sql_keymap_redirect_migration(context: Context, **kwargs) -> bool:
    return requires_migration(context, Script.SQL_KEYMAP_REDIRECT.value, None, **kwargs)


def sql_keymap_redirect_migration(context: Context, **kwargs):
    apply_migration_to_outdated_keymaps(context, Script.SQL_KEYMAP_REDIRECT.value, apply_migration, **kwargs)


def apply_migration(context: Context, keymap: "SqlAlchemyKeyMap", migration: str):
    insp = sa.inspect(keymap.engine)
    table_names = insp.get_table_names()
    keymap.metadata.reflect()
    for table in table_names:
        if table.startswith("_"):
            continue

        columns = insp.get_columns(table)
        column_names = [col["name"] for col in columns]
        if set(column_names) == {"key", "hash", "value"}:
            km_table = keymap.get_table(table)
            migrate_table(keymap, km_table)
            reset_keymap_increment(context, keymap, table)


def migrate_table(keymap: "SqlAlchemyKeyMap", table: sa.Table):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from spinta.datasets.keymaps.sqlalchemy import prepare_value

    connection = keymap.conn
    ctx = MigrationContext.configure(connection, opts={"target_metadata": keymap.metadata, "transactional_ddl:": True})
    op = Operations(ctx)

    table_name = table.name
    temp_table = f"t_{table_name}"
    deleted_table = f"d_{table_name}"

    # Store table state, since reflecting during transaction sometimes fails
    temp_table_exists = False
    deleted_table_exists = False
    normal_table_exists = True
    progress = tqdm(desc=f'MIGRATING "{table}" KEYMAP DATA', ascii=True)
    try:
        with connection.begin():
            if temp_table in keymap.metadata.tables:
                temp_table_exists = True

            new_table = keymap._create_table(temp_table)
            temp_table_exists = True

            count_stmt = sa.select(sa.func.count()).select_from(table)
            count = connection.execute(count_stmt).scalar()
            progress.total = count

            data_stmt = sa.select([table.c.key, table.c.value])
            for row in connection.execute(data_stmt):
                decoded = msgpack.loads(row["value"], raw=False)
                decoded = prepare_value(decoded)
                connection.execute(new_table.insert(values={"key": row["key"], "value": decoded}))
                progress.update(1)
            op.rename_table(table_name, deleted_table)
            normal_table_exists = False
            deleted_table_exists = True
            op.rename_table(temp_table, table_name)
            normal_table_exists = True
            temp_table_exists = False
            op.drop_table(deleted_table)
            deleted_table_exists = False
    except Exception:
        progress.close()

        with connection.begin():
            if temp_table_exists:
                op.drop_table(temp_table)

            if deleted_table_exists:
                if normal_table_exists:
                    op.drop_table(table_name)
                op.rename_table(deleted_table, table_name)

        raise
