import logging
import sys
from typing import Optional, List

from typer import Context as TyperContext, Argument
from typer import Option
from typer import echo

from spinta.cli.helpers.script.core import run_all_scripts, run_specific_script
from spinta.cli.helpers.script.helpers import sort_scripts_by_required
from spinta.cli.helpers.store import load_config
from spinta.cli.helpers.upgrade.components import UPGRADE_SCRIPT_TYPE
from spinta.cli.helpers.upgrade.registry import upgrade_script_registry
from spinta.core.context import configure_context
from spinta.exceptions import UpgradeError

log = logging.getLogger(__name__)


def upgrade(
    ctx: TyperContext,
    scripts: Optional[List[str]] = Argument(
        None,
        help=(
            f"""
        Specify a scripts to run.
        Available scripts: {upgrade_script_registry.get_all_names()}
        """
        ),
    ),
    ensure_config_dir: bool = Option(True, "--ensure-config", help=("Ensures that all config files are created.")),
    force: bool = Option(False, "-f", "--force", help=("Skips all checks when running upgrades.")),
    destructive: bool = Option(
        False,
        "-d",
        "--destructive",
        help=(
            """
        Runs scripts in destructive mode (scripts can now override already migrated files).
        WARNING: only use this if you know what will be changed (recommended to create backups).
        """
        ),
    ),
    check_only: bool = Option(
        False,
        "-c",
        "--check",
        help=("Only runs script checks, skipping execution part (used to find out what scripts are needed to run)."),
    ),
):
    rc = ctx.obj.get("rc")
    rc.add("upgrade", {"upgrade_mode": True})
    context = configure_context(ctx.obj)

    if force and check_only:
        echo("Cannot run force mode with check only mode", err=True)
        sys.exit(1)

    try:
        load_config(context, ensure_config_dir=ensure_config_dir)
    except UpgradeError:
        # Ignore UpgradeErrors, since this functions handles them
        pass

    if scripts is None:
        run_all_scripts(
            context=context,
            script_type=UPGRADE_SCRIPT_TYPE,
            destructive=destructive,
            force=force,
            check_only=check_only,
        )
        return

    script_objects = {}
    for script in scripts:
        # This automatically checks if script exists
        script_ = upgrade_script_registry.get(script)
        script_objects[script] = script_

    script_objects = sort_scripts_by_required(script_objects)
    for script in script_objects:
        run_specific_script(
            context=context,
            script_type=UPGRADE_SCRIPT_TYPE,
            destructive=destructive,
            force=force,
            script_name=script,
            check_only=check_only,
        )
