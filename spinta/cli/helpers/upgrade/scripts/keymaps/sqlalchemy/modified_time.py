from typing import TYPE_CHECKING

import sqlalchemy as sa

from spinta.cli.helpers.upgrade.components import Script
from spinta.cli.helpers.upgrade.scripts.keymaps.sqlalchemy.helpers import (
    requires_migration,
    apply_migration_to_outdated_keymaps,
    reset_keymap_increment,
)
from spinta.components import Context

if TYPE_CHECKING:
    from spinta.datasets.keymaps.sqlalchemy import SqlAlchemyKeyMap


def requires_sql_keymap_modified_migration(context: Context, **kwargs) -> bool:
    return requires_migration(context, Script.SQL_KEYMAP_MODIFIED.value, None, **kwargs)


def sql_keymap_modified_migration(context: Context, **kwargs):
    apply_migration_to_outdated_keymaps(context, Script.SQL_KEYMAP_MODIFIED.value, apply_migration, **kwargs)


def apply_migration(context: Context, keymap: "SqlAlchemyKeyMap", migration: str):
    insp = sa.inspect(keymap.engine)
    table_names = insp.get_table_names()
    keymap.metadata.reflect()
    for table in table_names:
        if table.startswith("_"):
            continue

        columns = insp.get_columns(table)
        column_names = [col["name"] for col in columns]
        if set(column_names) == {"key", "value", "redirect"}:
            km_table = keymap.get_table(table)
            migrate_table(keymap, km_table)
            reset_keymap_increment(context, keymap, table)


def migrate_table(keymap: "SqlAlchemyKeyMap", table: sa.Table):
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    connection = keymap.conn
    ctx = MigrationContext.configure(connection, opts={"target_metadata": keymap.metadata, "transactional_ddl:": True})
    op = Operations(ctx)
    table_name = table.name
    op.add_column(table_name=table_name, column=sa.Column("modified_at", sa.DateTime, index=True))
