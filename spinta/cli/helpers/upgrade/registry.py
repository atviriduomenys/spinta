from __future__ import annotations

from typing import List, Dict

from spinta.cli.helpers.upgrade.components import UpgradeComponent, Script, ScriptTarget, ScriptTag
from spinta.cli.helpers.upgrade.scripts.changelog import cli_requires_changelog_migrations, migrate_changelog_duplicates
from spinta.cli.helpers.upgrade.scripts.clients import migrate_clients, cli_requires_clients_migration
from spinta.cli.helpers.upgrade.scripts.deduplicate import cli_requires_deduplicate_migrations, migrate_duplicates
from spinta.cli.helpers.upgrade.scripts.keymaps.sqlalchemy.initial_setup import requires_sql_keymap_initial_migration, \
    sql_keymap_initial_migration
from spinta.cli.helpers.upgrade.scripts.keymaps.sqlalchemy.redirect_support import \
    requires_sql_keymap_redirect_migration, sql_keymap_redirect_migration
from spinta.cli.helpers.upgrade.scripts.redirect import cli_requires_redirect_migration, migrate_redirect

# Global upgrade script registry
# To add new script, just create `UpgradeComponent` and add it using register_script
# If there are no checks needed, just pass None to `check`
# If script requires other scripts to pass, you can add their key in `required` field

__upgrade_scripts: Dict[str, UpgradeComponent] = {}


def _register_script(script: UpgradeComponent):
    if script_exists(script.name):
        raise Exception("ALREADY REGISTERED WITH SAME NAME")

    __upgrade_scripts[script.name] = script


def get_all_script_names() -> List[str]:
    return list(__upgrade_scripts.keys())


def get_all_scripts() -> dict[str, UpgradeComponent]:
    return __upgrade_scripts


def get_filtered_scripts(targets: set | None = None, tags: set | None = None) -> dict[str, UpgradeComponent]:
    if not targets and not tags:
        return __upgrade_scripts

    result = {}
    for key, script in __upgrade_scripts.items():
        if targets and not targets.issubset(script.targets):
            continue

        if tags and not tags.issubset(script.tags):
            continue

        result[key] = script
    return result


def get_script(name: str) -> UpgradeComponent:
    if not script_exists(name):
        raise Exception("NO SCRIPT WITH NAME IS REGISTERED")

    return __upgrade_scripts[name]


def script_exists(
    script_name: str
) -> bool:
    return script_name in __upgrade_scripts


_register_script(
    UpgradeComponent(
        name=Script.CLIENTS.value,
        upgrade=migrate_clients,
        check=cli_requires_clients_migration,
        targets={
            ScriptTarget.AUTH.value,
            ScriptTarget.FS.value
        },
    )
)
_register_script(
    UpgradeComponent(
        name=Script.REDIRECT.value,
        upgrade=migrate_redirect,
        check=cli_requires_redirect_migration,
        targets={ScriptTarget.BACKEND.value},
    )
)
_register_script(
    UpgradeComponent(
        name=Script.DEDUPLICATE.value,
        upgrade=migrate_duplicates,
        check=cli_requires_deduplicate_migrations,
        required=[Script.REDIRECT.value],
        targets={ScriptTarget.BACKEND.value},
        tags={ScriptTag.BUG_FIX.value},
    )
)
_register_script(
    UpgradeComponent(
        name=Script.CHANGELOG.value,
        upgrade=migrate_changelog_duplicates,
        check=cli_requires_changelog_migrations,
        required=[Script.DEDUPLICATE.value],
        targets={ScriptTarget.BACKEND.value}
    )
)

# Sqlalchemy keymap migrations
# You can build migration dependency chain using `required` list
_register_script(
    UpgradeComponent(
        name=Script.SQL_KEYMAP_INITIAL.value,
        upgrade=sql_keymap_initial_migration,
        check=requires_sql_keymap_initial_migration,
        targets={ScriptTarget.SQLALCHEMY_KEYMAP.value},
        tags={ScriptTag.DB_MIGRATION.value},
    )
)
_register_script(
    UpgradeComponent(
        name=Script.SQL_KEYMAP_REDIRECT.value,
        upgrade=sql_keymap_redirect_migration,
        check=requires_sql_keymap_redirect_migration,
        required=[Script.SQL_KEYMAP_INITIAL.value],
        targets={ScriptTarget.SQLALCHEMY_KEYMAP.value},
        tags={ScriptTag.DB_MIGRATION.value},
    )
)
