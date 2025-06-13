from collections.abc import Callable

from spinta.components import Context, Store

from typer import echo


def outdated_keymaps(context: Context, migration: str, additional_check: Callable = None, **kwargs):
    from spinta.datasets.keymaps.sqlalchemy import SqlAlchemyKeyMap

    store: Store = context.get('store')
    for key, keymap in store.keymaps.items():
        if not isinstance(keymap, SqlAlchemyKeyMap):
            continue

        with keymap:
            contains = keymap.contains_migration(migration)
            if not contains:
                yield keymap
                continue

            if additional_check and additional_check(context, **kwargs):
                yield keymap
                continue


def apply_migration_to_outdated_keymaps(context: Context, migration: str, apply_migration: callable, **kwargs):
    keymaps = outdated_keymaps(context, migration, None, **kwargs)
    for keymap in keymaps:
        echo(f"\tApplying \"{migration}\" migration to keymap (\"{keymap.name}\")")
        apply_migration(context, keymap, migration)
        keymap.mark_migration(migration)


def requires_migration(context: Context, migration: str, additional_check: Callable = None, **kwargs) -> bool:
    keymaps = outdated_keymaps(context, migration, additional_check, **kwargs)
    for _ in keymaps:
        return True

    return False
