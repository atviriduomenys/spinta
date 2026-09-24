from typing import Any, Callable

import sqlalchemy as sa

from spinta import commands
from spinta.cli.helpers.script.components import ScriptTag, ScriptTarget
from spinta.cli.helpers.script.helpers import sort_scripts_by_required
from spinta.cli.helpers.upgrade.components import UpgradeScript
from spinta.cli.helpers.upgrade.registry import upgrade_script_registry
from spinta.components import Context
from spinta.exceptions import UpgradeError
from spinta.utils.sqlite import SqliteDatabase, SqliteMigrations


def get_target_migrations(target: ScriptTarget) -> dict[str, UpgradeScript]:
    migration_scripts = upgrade_script_registry.get_all(targets={target.value}, tags={ScriptTag.DB_MIGRATION.value})
    return sort_scripts_by_required(migration_scripts)


def is_fresh_database(context: Context, db: SqliteDatabase, *, table_namer: Callable[[Any], str] = lambda t: t) -> bool:
    insp = sa.inspect(db.engine)
    tables = insp.get_table_names()

    if not len(tables):
        return True

    for metatable_name in db.metatable_templates.keys():
        if metatable_name in tables:
            return False

    tables = [table for table in tables if not table.startswith("_")]
    manifest = context.get("store").manifest
    for table in tables:
        if commands.has_model(context, manifest, table_namer(table)):
            return False

    return True


def ensure_migrated(
    context: Context,
    book: SqliteMigrations,
    target: ScriptTarget,
    error_factory: Callable[[str], UpgradeError],
    table_namer: Callable[[Any], str] = lambda t: t,
):
    def _mark_migrations():
        for script in get_target_migrations(target).keys():
            book.mark_migration(script)

    upgrade_mode = False
    if context.has("config"):
        config = context.get("config")
        upgrade_mode = config.upgrade_mode

    is_fresh = is_fresh_database(context, book.db, table_namer=table_namer)
    # Initialize missing metadata tables
    book.db.create_all_metatables()

    if is_fresh:
        if book.db.is_entered:
            _mark_migrations()
        else:
            with book.db:
                _mark_migrations()
    else:
        if upgrade_mode:
            return
        for script in get_target_migrations(target).values():
            if script.check(context, target_mapping={target.value: {book.db.dsn}}):
                raise error_factory(script.name)
