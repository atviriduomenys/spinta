from __future__ import annotations

import logging
import pathlib
import sys
from typing import Optional, List

from typer import Context as TyperContext, Argument
from typer import Option
from typer import echo

from spinta.cli.helpers.admin.components import ADMIN_SCRIPT_TYPE
from spinta.cli.helpers.admin.registry import admin_script_registry
from spinta.cli.helpers.script.core import run_specific_script
from spinta.cli.helpers.script.helpers import sort_scripts_by_required
from spinta.cli.helpers.store import load_config
from spinta.core.context import configure_context

log = logging.getLogger(__name__)


def admin(
    ctx: TyperContext,
    scripts: Optional[List[str]] = Argument(
        None,
        help=(
            f"""
        Specify a scripts to run.
        Available scripts: {admin_script_registry.get_all_names()}
        """
        ),
    ),
    ensure_config_dir: bool = Option(True, "--ensure-config", help=("Ensures that all config files are created.")),
    force: bool = Option(False, "-f", "--force", help=("Skips all checks when running scripts.")),
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
    input_path: Optional[pathlib.Path] = Option(
        None,
        "-i",
        "--input",
        help=("Path to input file (some scripts might require extra data). If not given, script will read from stdin."),
    ),
):
    context = configure_context(ctx.obj)

    if force and check_only:
        echo("Cannot run force mode with check only mode", err=True)
        sys.exit(1)

    load_config(context, ensure_config_dir=ensure_config_dir)

    if scripts is None:
        echo("At least one script needs to be specified", err=True)
        sys.exit(1)

    script_objects = {}
    for script in scripts:
        # This automatically checks if script exists
        script_ = admin_script_registry.get(script)
        script_objects[script] = script_

    script_objects = sort_scripts_by_required(script_objects)
    for script in script_objects:
        run_specific_script(
            context=context,
            script_type=ADMIN_SCRIPT_TYPE,
            destructive=destructive,
            force=force,
            script_name=script,
            check_only=check_only,
            input_path=input_path,
        )
