from typing import Callable

from spinta.cli.helpers.message import cli_message
from spinta.cli.helpers.push.components import PushState
from spinta.cli.helpers.push.utils import default_push_state_dir
from spinta.cli.helpers.script.components import ScriptTarget
from spinta.components import Context
from spinta.utils.sqlite import apply_migration_to_outdated_db, outdated_sqlite_db


def requires_migration(
    context: Context,
    migration: str,
    additional_check: Callable | None = None,
    target_mapping: dict[str, set[str]] | None = None,
    **kwargs,
) -> bool:
    push_states = get_push_states(context, target_mapping)
    if push_states is None:
        cli_message(
            f"Skipped {migration!r} check, please provide --target {ScriptTarget.PUSH_STATE_DB.value}=<path> in order to run this migration"
        )
        return False

    for state in push_states:
        if outdated_sqlite_db(context, state.migrations, migration, additional_check, **kwargs):
            return True
    return False


def apply_migration_to_push_state(
    context: Context,
    migration: str,
    apply_migration: Callable,
    target_mapping: dict[str, set[str]] | None = None,
    **kwargs,
):
    push_states = get_push_states(context, target_mapping)

    if push_states is None:
        cli_message(
            f"Skipped {migration!r} check, please provide --target {ScriptTarget.PUSH_STATE_DB.value}=<path> in order to run this migration"
        )
        return

    for state in push_states:
        with state:
            apply_migration_to_outdated_db(
                context, state, migration, apply_migration, state.db.dsn or "push_state_db", **kwargs
            )


def get_push_states(
    context: Context,
    target_mapping: dict[str, set[str]] | None = None,
) -> list[PushState] | None:
    push_state_targets = (
        target_mapping.get(ScriptTarget.PUSH_STATE_DB.value, set()) if target_mapping is not None else set()
    )

    if not push_state_targets:
        config = context.get("config")
        push_state_dir = default_push_state_dir(config)
        if not push_state_dir.exists() or not push_state_dir.is_dir():
            return None

        for file in push_state_dir.iterdir():
            if file.is_file() and file.suffix == ".db":
                push_state_targets.add(str(file))

        if not push_state_targets:
            return None

    result = list()
    for push_state_target in push_state_targets:
        if not push_state_target.startswith("sqlite"):
            push_state_target = f"sqlite+spinta:///{push_state_target}"

        result.append(PushState(push_state_target))
    return result
