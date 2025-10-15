from __future__ import annotations

import pathlib
import sys
from collections import defaultdict, deque

from typer import echo

from spinta.cli.helpers.script.components import ScriptStatus
from spinta.cli.helpers.upgrade.components import UpgradeScript
from spinta.components import Context, Store


def sort_scripts_by_required(scripts: dict[str, UpgradeScript]) -> dict:
    graph = defaultdict(list)
    requirement_count = defaultdict(int)

    data = scripts
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
        echo(f"Warning: Dependency cycle detected or unresolved dependencies in: {unresolved}", err=True)
        # Extend results, potentially might cause errors, because of cycles
        result.extend(unresolved)

    return {res: data[res] for res in result}


def script_check_status_message(script_name: str, status: ScriptStatus) -> str:
    return f"Script {script_name!r} check. Status: {status.value}"


def script_destructive_warning(script_name: str, message: str) -> str:
    return f"WARNING (DESTRUCTIVE MODE). Script {script_name!r} will {message}."


def ensure_store_is_loaded(context: Context, verbose: bool = False) -> Store:
    from spinta.cli.helpers.store import prepare_manifest

    if store := context.get("store"):
        if store.manifest:
            return store

    store = prepare_manifest(context, verbose=verbose, full_load=True)
    return store


def parse_input_path(
    context: Context,
    input_path: pathlib.Path = None,
    required: bool = True,
    **kwargs,
) -> list[str] | None:
    if input_path is None:
        # Reads stdin direct for data
        if not sys.stdin.isatty():
            data = sys.stdin.read()
            data = data.splitlines()
            return data

        if not required:
            return None

        echo("Script requires model list file path (can also add it through `--input <file_path>` argument).", err=True)
        input_path = input("Enter model list file path: ")

    if not input_path.exists():
        echo(f'File "{input_path}" does not exist.', err=True)
        sys.exit(1)

    with input_path.open("r") as f:
        return f.read().splitlines()
