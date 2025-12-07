from typing import List

from spinta import commands
from spinta.components import Context
from spinta.utils.scopes import name_to_scope


@commands.get_model_scopes.register(Context, str, list)
def get_model_scopes(context: Context, model: str, actions: List[str]):
    config = context.get("config")
    return [
        name_to_scope(
            "{prefix}{name}/:{action}" if is_udts else "{prefix}{name}_{action}",
            model,
            maxlen=config.scope_max_length,
            params={
                "prefix": config.scope_prefix_udts if is_udts else config.scope_prefix,
                "action": "create" if is_udts and action == "insert" else action,
            },
            is_udts=is_udts,
        )
        for action in actions
        for is_udts in [False, True]
    ]
