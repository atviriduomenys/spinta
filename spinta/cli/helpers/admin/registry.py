from __future__ import annotations

from spinta.cli.helpers.admin.components import ADMIN_SCRIPT_TYPE, AdminScript, Script
from spinta.cli.helpers.admin.scripts.add_local_ids import add_local_ids
from spinta.cli.helpers.admin.scripts.changelog import cli_requires_changelog_migrations, migrate_changelog_duplicates
from spinta.cli.helpers.admin.scripts.citus_shard import cli_requires_citus_distribution, migrate_citus_distributions
from spinta.cli.helpers.admin.scripts.deduplicate import cli_requires_deduplicate_migrations, migrate_duplicates
from spinta.cli.helpers.admin.scripts.enums import gather_invalid_enum_values
from spinta.cli.helpers.admin.scripts.remove_local_ids import remove_local_ids
from spinta.cli.helpers.script.components import ScriptTag, ScriptTarget
from spinta.cli.helpers.script.registry import script_registry
from spinta.cli.helpers.upgrade.components import UPGRADE_SCRIPT_TYPE
from spinta.cli.helpers.upgrade.components import Script as UpgradeScript

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
script_registry.register(
    AdminScript(name=Script.ENUM_LIST.value, run=gather_invalid_enum_values, targets={ScriptTarget.BACKEND.value})
)
script_registry.register(
    AdminScript(
        name=Script.CITUS_DISTRIBUTION.value,
        run=migrate_citus_distributions,
        check=cli_requires_citus_distribution,
        required=[(UPGRADE_SCRIPT_TYPE, UpgradeScript.POSTGRESQL_SCHEMAS.value)],
        targets={ScriptTarget.BACKEND.value},
    )
)
script_registry.register(AdminScript(name=Script.ADD_LOCAL_IDS.value, run=add_local_ids, required=[]))
script_registry.register(AdminScript(name=Script.REMOVE_LOCAL_IDS.value, run=remove_local_ids, required=[]))
