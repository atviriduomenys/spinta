from spinta.cli.helpers.upgrade.components import ScriptStatus
from spinta.components import Context, Store


def script_check_status_message(
    script_name: str,
    status: ScriptStatus
) -> str:
    return f"Script '{script_name}' upgrade check. Status: {status.value}"


def script_destructive_warning(
    script_name: str,
    message: str
) -> str:
    return f"WARNING (DESTRUCTIVE MODE). Script '{script_name}' will {message}."


def ensure_store_is_loaded(
    context: Context
) -> Store:
    from spinta.cli.helpers.store import prepare_manifest

    if store := context.get('store'):
        if store.manifest:
            return store

    store = prepare_manifest(context, full_load=True)
    return store
