from spinta.cli.helpers.push.components import PushState
from spinta.cli.helpers.upgrade.components import Script
from spinta.cli.helpers.upgrade.scripts.push_state.helpers import apply_migration_to_push_state, requires_migration
from spinta.components import Context


def requires_push_state_initial_migration(context: Context, **kwargs) -> bool:
    return requires_migration(context, Script.PUSH_STATE_INITIAL.value, None, **kwargs)


def push_state_initial_migration(context: Context, **kwargs):
    apply_migration_to_push_state(context, Script.PUSH_STATE_INITIAL.value, apply_migration, **kwargs)


def apply_migration(context: Context, push_state: PushState, migration: str):
    push_state.create_all_metatables()
