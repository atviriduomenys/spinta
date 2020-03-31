from spinta import commands
from spinta.auth import check_generated_scopes
from spinta.components import Context, Action
from spinta.datasets.components import Entity


@commands.authorize.register()
def authorize(context: Context, action: Action, entity: Entity):
    check_generated_scopes(context, entity.model.model_type(), action.value + '_external')
