from typing import List, Dict
from typer import echo

from spinta.cli.helpers.upgrade.clients import migrate_clients, cli_requires_clients_migration
from spinta.cli.helpers.upgrade.components import UpgradeComponent, UPGRADE_CLIENTS_SCRIPT
from spinta.cli.helpers.upgrade.helpers import script_check_status_message
from spinta.components import Context

# Global upgrade script mapper
# To add new script, just create script name and create `UpgradeComponent` for it
# If there are no checks needed, just pass None to `check`
__upgrade_scripts: Dict[str, UpgradeComponent] = {
    UPGRADE_CLIENTS_SCRIPT: UpgradeComponent(
        upgrade=migrate_clients,
        check=cli_requires_clients_migration
    )
}

UPGRADE_CHECK_STATUS_PASSED = "PASSED"
UPGRADE_CHECK_STATUS_REQUIRED = "REQUIRED"
UPGRADE_CHECK_STATUS_FORCED = "FORCED"


# Returns a list of all available script names
def get_all_upgrade_script_names() -> List[str]:
    return list(__upgrade_scripts.keys())


def run_all_scripts(
    context: Context,
    destructive: bool = False,
    force: bool = False,
    **kwargs
):
    for script_name in __upgrade_scripts.keys():
        run_specific_script(
            context=context,
            script_name=script_name,
            destructive=destructive,
            force=force,
            **kwargs
        )


def does_script_exist(
    script_name: str
) -> bool:
    return script_name in __upgrade_scripts


def run_specific_script(
    context: Context,
    script_name: str,
    destructive: bool = False,
    force: bool = False,
    **kwargs
):
    script = __upgrade_scripts[script_name]

    status = UPGRADE_CHECK_STATUS_PASSED
    if force or script.check(context, **kwargs):
        status = UPGRADE_CHECK_STATUS_FORCED if force else UPGRADE_CHECK_STATUS_REQUIRED
        echo(script_check_status_message(script_name, status))
        script.upgrade(context, destructive=destructive, **kwargs)
    else:
        echo(script_check_status_message(script_name, status))
