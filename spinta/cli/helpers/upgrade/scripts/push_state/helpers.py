from typing import Callable

from spinta.cli.helpers.message import cli_message
from spinta.cli.helpers.push.components import PUSH_STATE_PATH, PushState
from spinta.components import Context
from spinta.utils.sqlite import apply_migration_to_outdated_db, outdated_sqlite_db


def requires_migration(context: Context, migration: str, additional_check: Callable | None = None, **kwargs) -> bool:
    push_state = get_push_state(context)

    if push_state is None:
        cli_message(
            f"Skipped {migration!r} check, please provide -o push_state_path=<path> in order to run this migration"
        )
        return False

    return outdated_sqlite_db(context, push_state, migration, additional_check, **kwargs)


def apply_migration_to_push_state(context: Context, migration: str, apply_migration: Callable, **kwargs):
    push_state = get_push_state(context)

    if push_state is None:
        cli_message(
            f"Skipped {migration!r} check, please provide -o push_state_path=<path> in order to run this migration"
        )
        return

    with push_state:
        apply_migration_to_outdated_db(
            context, push_state, migration, apply_migration, push_state.dsn or "push_state_db", **kwargs
        )


def get_push_state(context: Context) -> PushState | None:
    rc = context.get("rc")
    push_state_path = rc.get(PUSH_STATE_PATH)

    # RawConfig given push state path is only file path
    if push_state_path:
        push_state_path = f"sqlite+spinta:///{push_state_path}"
        return PushState(push_state_path)

    # Context given push state path is set when calling init_push_state, which set dsn and not direct path
    if context.has(PUSH_STATE_PATH):
        push_state_path = context.get(PUSH_STATE_PATH)
        return PushState(push_state_path)

    return None
