from __future__ import annotations

from typer import echo

from spinta.cli.helpers.upgrade.components import UpgradeComponent, Script, ScriptStatus
from spinta.cli.helpers.upgrade.helpers import script_check_status_message, sort_scripts_by_required
from spinta.cli.helpers.upgrade.registry import get_script, script_exists, get_all_scripts
from spinta.components import Context


def run_all_scripts(
    context: Context,
    destructive: bool = False,
    force: bool = False,
    check_only: bool = False,
    **kwargs
):
    scripts = get_all_scripts()
    sorted_scripts = sort_scripts_by_required(scripts)
    for script_name in sorted_scripts.keys():
        run_specific_script(
            context=context,
            script_name=script_name,
            destructive=destructive,
            force=force,
            check_only=check_only,
            **kwargs
        )


def run_specific_script(
    context: Context,
    script_name: str,
    destructive: bool = False,
    force: bool = False,
    check_only: bool = False,
    **kwargs
):
    script = get_script(script_name)
    status = check_script(context, script, **kwargs)
    if force:
        status = ScriptStatus.FORCED

    if status in (ScriptStatus.FORCED, ScriptStatus.REQUIRED) and not check_only:
        script.upgrade(context, destructive=destructive, **kwargs)
    echo(script_check_status_message(script_name, status))


def check_script(context: Context, script: str | Script | UpgradeComponent, **kwargs) -> ScriptStatus:
    if not isinstance(script, UpgradeComponent):
        if isinstance(script, Script):
            script = script.value

        if not script_exists(script):
            echo(f'Warning: "{script}" script not found')
            return ScriptStatus.SKIPPED

        script = get_script(script)

    if script.required:
        for required_script in script.required:
            if check_script(context, required_script, **kwargs) is ScriptStatus.REQUIRED:
                echo(f'Warning: "{required_script}" requirement is not met')
                return ScriptStatus.SKIPPED

    return ScriptStatus.REQUIRED if script.check(context, **kwargs) else ScriptStatus.PASSED
