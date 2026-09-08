from collections.abc import Callable
from typing import TYPE_CHECKING

from spinta.components import Context, Store
from spinta.utils.sqlite import apply_migration_to_outdated_db, outdated_sqlite_db

if TYPE_CHECKING:
    from spinta.datasets.keymaps.sqlalchemy import SqlAlchemyKeyMap


def outdated_keymaps(context: Context, migration: str, additional_check: Callable | None = None, **kwargs):
    from spinta.datasets.keymaps.sqlalchemy import SqlAlchemyKeyMap

    store: Store = context.get("store")
    for key, keymap in store.keymaps.items():
        if not isinstance(keymap, SqlAlchemyKeyMap):
            continue

        if outdated_sqlite_db(context, keymap, migration, additional_check, **kwargs):
            yield keymap


def apply_migration_to_outdated_keymaps(context: Context, migration: str, apply_migration: Callable, **kwargs):
    keymaps = outdated_keymaps(context, migration, None, **kwargs)
    for keymap in keymaps:
        apply_migration_to_outdated_db(context, keymap, migration, apply_migration, keymap.name, **kwargs)


def requires_migration(context: Context, migration: str, additional_check: Callable | None = None, **kwargs) -> bool:
    keymaps = outdated_keymaps(context, migration, additional_check, **kwargs)
    for _ in keymaps:
        return True

    return False


def reset_keymap_increment(context: Context, keymap: "SqlAlchemyKeyMap", key: str):
    table = keymap.get_table(keymap.sync_table_name)
    keymap.conn.execute(table.update().values(cid=0, updated=None).where(table.c.model == key))
