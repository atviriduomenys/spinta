from __future__ import annotations

from spinta.cli.helpers.admin.components import AdminScript, Script, ADMIN_SCRIPT_TYPE
from spinta.cli.helpers.admin.scripts.changelog import migrate_changelog_duplicates, cli_requires_changelog_migrations
from spinta.cli.helpers.admin.scripts.deduplicate import migrate_duplicates, cli_requires_deduplicate_migrations
from spinta.cli.helpers.script.components import ScriptTarget, ScriptTag
from spinta.cli.helpers.script.registry import script_registry
from spinta.cli.helpers.upgrade.components import Script as UpgradeScript, UPGRADE_SCRIPT_TYPE

# For convenience, easier access to admin scripts
admin_script_registry = script_registry.view(ADMIN_SCRIPT_TYPE)

script_registry.register(
    AdminScript(
        name=Script.DEDUPLICATE.value,
        run=migrate_duplicates,
        check=cli_requires_deduplicate_migrations,
        required=[(UPGRADE_SCRIPT_TYPE, UpgradeScript.REDIRECT.value)],
        targets={ScriptTarget.BACKEND.value},
        tags={ScriptTag.BUG_FIX.value},
    )
)
script_registry.register(
    AdminScript(
        name=Script.CHANGELOG.value,
        run=migrate_changelog_duplicates,
        check=cli_requires_changelog_migrations,
        required=[Script.DEDUPLICATE.value],
        targets={ScriptTarget.BACKEND.value},
    )
)
