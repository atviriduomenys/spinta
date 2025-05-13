from __future__ import annotations
from collections import defaultdict, deque

from typing import List, Dict
from typer import echo

from spinta.cli.helpers.upgrade.scripts.clients import migrate_clients, cli_requires_clients_migration
from spinta.cli.helpers.upgrade.components import UpgradeComponent, Script, ScriptStatus
from spinta.cli.helpers.upgrade.helpers import script_check_status_message
from spinta.cli.helpers.upgrade.scripts.deduplicate import cli_requires_deduplicate_migrations, migrate_duplicates
from spinta.cli.helpers.upgrade.scripts.redirect import cli_requires_redirect_migration, migrate_redirect
from spinta.components import Context

# Global upgrade script mapper
# To add new script, just create script name and create `UpgradeComponent` for it
# If there are no checks needed, just pass None to `check`
# If script requires other scripts to pass, you can add their key in `required` field
__upgrade_scripts: Dict[str, UpgradeComponent] = {
    Script.CLIENTS.value: UpgradeComponent(
        upgrade=migrate_clients,
        check=cli_requires_clients_migration
    ),
    Script.REDIRECT.value: UpgradeComponent(
        upgrade=migrate_redirect,
        check=cli_requires_redirect_migration
    ),
    Script.DEDUPLICATE.value: UpgradeComponent(
        upgrade=migrate_duplicates,
        check=cli_requires_deduplicate_migrations,
        required=[Script.REDIRECT.value]
    )
}


# Returns a list of all available script names
def get_all_upgrade_script_names() -> List[str]:
    return list(__upgrade_scripts.keys())


def run_all_scripts(
    context: Context,
    destructive: bool = False,
    force: bool = False,
    check_only: bool = False,
    **kwargs
):
    sorted_scripts = _sort_scripts_by_required()
    for script_name in sorted_scripts.keys():
        run_specific_script(
            context=context,
            script_name=script_name,
            destructive=destructive,
            force=force,
            check_only=check_only,
            **kwargs
        )


def script_exists(
    script_name: str
) -> bool:
    return script_name in __upgrade_scripts


def run_specific_script(
    context: Context,
    script_name: str,
    destructive: bool = False,
    force: bool = False,
    check_only: bool = False,
    **kwargs
):
    script = __upgrade_scripts[script_name]
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

        script = __upgrade_scripts[script]

    if script.required:
        for required_script in script.required:
            if check_script(context, required_script, **kwargs) is ScriptStatus.REQUIRED:
                echo(f'Warning: "{required_script}" requirement is not met')
                return ScriptStatus.SKIPPED

    return ScriptStatus.REQUIRED if script.check(context, **kwargs) else ScriptStatus.PASSED


def _sort_scripts_by_required() -> dict:
    graph = defaultdict(list)
    requirement_count = defaultdict(int)

    data = __upgrade_scripts
    # initialize requirements
    for node in data:
        requirement_count[node] = 0

    # Build graph and requirement_count
    for node, comp in data.items():
        required = comp.required
        if not required:
            continue

        for req in required:
            if req not in data:
                echo(f'Warning: "{req}" requirement for "{node}" script was not found')
                continue
            graph[req].append(node)
            requirement_count[node] += 1

    # Collect nodes without dependencies
    queue = deque([node for node in data if requirement_count[node] == 0])
    result = []

    while queue:
        current = queue.popleft()
        result.append(current)
        for neighbor in graph[current]:
            requirement_count[neighbor] -= 1
            if requirement_count[neighbor] == 0:
                queue.append(neighbor)

    if len(result) != len(data):
        unresolved = set(data) - set(result)
        echo(f'Warning: Dependency cycle detected or unresolved dependencies in: {unresolved}')
        # Extend results, potentially might cause errors, because of cycles
        result.extend(unresolved)

    return {res: data[res] for res in result}
