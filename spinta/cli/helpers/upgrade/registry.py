from __future__ import annotations

from spinta.cli.helpers.script.registry import script_registry

from spinta.cli.helpers.upgrade.components import Script, UpgradeScript, UPGRADE_SCRIPT_TYPE
from spinta.cli.helpers.script.components import ScriptTarget, ScriptTag
from spinta.cli.helpers.upgrade.scripts.clients import migrate_clients, cli_requires_clients_migration
from spinta.cli.helpers.upgrade.scripts.keymaps.sqlalchemy.initial_setup import (
    requires_sql_keymap_initial_migration,
    sql_keymap_initial_migration,
)
from spinta.cli.helpers.upgrade.scripts.keymaps.sqlalchemy.modified_time import (
    requires_sql_keymap_modified_migration,
    sql_keymap_modified_migration,
)
from spinta.cli.helpers.upgrade.scripts.keymaps.sqlalchemy.redirect_support import (
    requires_sql_keymap_redirect_migration,
    sql_keymap_redirect_migration,
)
from spinta.cli.helpers.upgrade.scripts.redirect import cli_requires_redirect_migration, migrate_redirect

# For convenience, easier access to upgrade scripts
upgrade_script_registry = script_registry.view(UPGRADE_SCRIPT_TYPE)

script_registry.register(
    UpgradeScript(
        name=Script.CLIENTS.value,
        run=migrate_clients,
        check=cli_requires_clients_migration,
        targets={ScriptTarget.AUTH.value, ScriptTarget.FS.value},
    )
)
script_registry.register(
    UpgradeScript(
        name=Script.REDIRECT.value,
        run=migrate_redirect,
        check=cli_requires_redirect_migration,
        targets={ScriptTarget.BACKEND.value},
    )
)

# Sqlalchemy keymap migrations
# You can build migration dependency chain using `required` list
script_registry.register(
    UpgradeScript(
        name=Script.SQL_KEYMAP_INITIAL.value,
        run=sql_keymap_initial_migration,
        check=requires_sql_keymap_initial_migration,
        targets={ScriptTarget.SQLALCHEMY_KEYMAP.value},
        tags={ScriptTag.DB_MIGRATION.value},
    )
)
script_registry.register(
    UpgradeScript(
        name=Script.SQL_KEYMAP_REDIRECT.value,
        run=sql_keymap_redirect_migration,
        check=requires_sql_keymap_redirect_migration,
        required=[Script.SQL_KEYMAP_INITIAL.value],
        targets={ScriptTarget.SQLALCHEMY_KEYMAP.value},
        tags={ScriptTag.DB_MIGRATION.value},
    )
)
script_registry.register(
    UpgradeScript(
        name=Script.SQL_KEYMAP_MODIFIED.value,
        run=sql_keymap_modified_migration,
        check=requires_sql_keymap_modified_migration,
        required=[Script.SQL_KEYMAP_REDIRECT.value],
        targets={ScriptTarget.SQLALCHEMY_KEYMAP.value},
        tags={ScriptTag.DB_MIGRATION.value},
    )
)
