from typing import List

from spinta import commands
from spinta.components import Context
from spinta.utils.scopes import name_to_scope


@commands.get_model_scopes.register(Context, str, list)
def get_model_scopes(context: Context, model: str, actions: List[str]):
    config = context.get('config')
    return [
        name_to_scope('{prefix}{name}_{action}', model, maxlen=config.scope_max_length, params={
            'prefix': config.scope_prefix,
            'action': action,
        })
        for action in actions
    ]
