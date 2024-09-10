import logging
from typer import Context as TyperContext
from typer import Option

from spinta.cli.helpers.store import load_config
from spinta.cli.helpers.upgrade.scripts import run_specific_script, run_all_scripts, get_all_upgrade_script_names, \
    does_script_exist
from spinta.core.context import configure_context
from spinta.exceptions import UpgradeError, UpgradeScriptNotFound

log = logging.getLogger(__name__)


def upgrade(
    ctx: TyperContext,
    ensure_config_dir: bool = Option(True, '--ensure-config', help=(
        "Ensures that all config files are created."
    )),
    force: bool = Option(False, '-f', '--force', help=(
        "Skips all checks when running upgrades."
    )),
    destructive: bool = Option(False, '-d', '--destructive', help=(
        """
        Runs scripts in destructive mode (scripts can now override already migrated files).
        WARNING: only use this if you know what will be changed (recommended to create backups).
        """
    )),
    run: str = Option(None, '-r', '--run', help=(
        f"""
        Specify a script to run.
        Available scripts: {get_all_upgrade_script_names()}
        """
    )),
):
    context = configure_context(ctx.obj)
    try:
        load_config(
            context,
            ensure_config_dir=ensure_config_dir
        )
    except UpgradeError:
        # Ignore UpgradeErrors, since this functions handles them
        pass

    if run is not None:
        if not does_script_exist(run):
            raise UpgradeScriptNotFound(
                script=run,
                available_scripts=get_all_upgrade_script_names()
            )

        run_specific_script(
            context=context,
            destructive=destructive,
            force=force,
            script_name=run
        )
        return

    run_all_scripts(
        context=context,
        destructive=destructive,
        force=force
    )
