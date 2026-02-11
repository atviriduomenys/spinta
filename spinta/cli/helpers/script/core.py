from __future__ import annotations

from click import echo

from spinta.cli.helpers.script.components import ScriptStatus, ScriptBase, ScriptStatusCache
from spinta.cli.helpers.script.helpers import sort_scripts_by_required, script_check_status_message
from spinta.cli.helpers.script.registry import script_registry
from spinta.cli.helpers.upgrade.components import Script
from spinta.components import Context


def run_all_scripts(
    context: Context,
    script_type: str,
    destructive: bool = False,
    force: bool = False,
    check_only: bool = False,
    status_cache: ScriptStatusCache | None = None,
    **kwargs,
):
    scripts = script_registry.get_all(script_type)
    sorted_scripts = sort_scripts_by_required(scripts)
    for script_name in sorted_scripts.keys():
        run_specific_script(
            context=context,
            script_type=script_type,
            script_name=script_name,
            destructive=destructive,
            force=force,
            check_only=check_only,
            status_cache=status_cache,
            **kwargs,
        )


def run_specific_script(
    context: Context,
    script_type: str,
    script_name: str,
    destructive: bool = False,
    force: bool = False,
    check_only: bool = False,
    status_cache: ScriptStatusCache | None = None,
    **kwargs,
):
    script = script_registry.get(script_type, script_name)
    status = check_script(context, script_type, script, status_cache, **kwargs)
    if status_cache is not None:
        # We do not want to cache skipped or forced status, since it indicates logic, rather than actual status.
        # Forced ignores all checks, while skipped just says that script requirements were not met (which can change over time).
        if status != ScriptStatus.SKIPPED:
            status_cache.set(script_type, script_name, status)

    if force:
        status = ScriptStatus.FORCED

    echo(script_check_status_message(script_name, status))
    if status in (ScriptStatus.FORCED, ScriptStatus.REQUIRED) and not check_only:
        script.run(context, destructive=destructive, **kwargs)
        if status_cache is not None:
            status_cache.set(script_type, script_name, ScriptStatus.PASSED)


def check_script(
    context: Context,
    script_type: str,
    script: str | Script | ScriptBase,
    status_cache: ScriptStatusCache | None,
    **kwargs,
) -> ScriptStatus:
    if not isinstance(script, ScriptBase):
        if isinstance(script, Script):
            script = script.value

        if not script_registry.contains(script_type, script):
            echo(f"Warning: {script_type!r} script {script!r} was not found", err=True)
            return ScriptStatus.SKIPPED

        script = script_registry.get(script_type, script)

    if status_cache is not None and (status := status_cache.get(script_type, script.name)) is not None:
        return status

    if script.required:
        for required_script in script.required:
            required_script_type = script_type
            if isinstance(required_script, tuple):
                required_script_type = required_script[0]
                required_script = required_script[1]

            if check_script(context, required_script_type, required_script, status_cache, **kwargs) in (
                ScriptStatus.REQUIRED,
                ScriptStatus.SKIPPED,
            ):
                echo(
                    f"Warning: {required_script_type!r} script {required_script!r} requirement is not met for {script.name!r} script",
                    err=True,
                )
                return ScriptStatus.SKIPPED

    return ScriptStatus.REQUIRED if script.check(context, **kwargs) else ScriptStatus.PASSED
