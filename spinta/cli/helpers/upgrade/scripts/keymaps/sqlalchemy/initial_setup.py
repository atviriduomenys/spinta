from spinta.cli.helpers.upgrade.components import Script
from spinta.cli.helpers.upgrade.scripts.keymaps.sqlalchemy.helpers import (
    requires_migration,
    apply_migration_to_outdated_keymaps,
)
from spinta.components import Context

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spinta.datasets.keymaps.sqlalchemy import SqlAlchemyKeyMap


def requires_sql_keymap_initial_migration(context: Context, **kwargs) -> bool:
    return requires_migration(context, Script.SQL_KEYMAP_INITIAL.value, None, **kwargs)


def sql_keymap_initial_migration(context: Context, **kwargs):
    apply_migration_to_outdated_keymaps(context, Script.SQL_KEYMAP_INITIAL.value, apply_migration, **kwargs)


def apply_migration(context: Context, keymap: "SqlAlchemyKeyMap", migration: str):
    # Initialize optional tables
    keymap.get_table(keymap.sync_table_name)
    keymap.get_table(keymap.migration_table_name)
