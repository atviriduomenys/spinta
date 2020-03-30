from starlette.requests import Request

from spinta.compat import urlparams_to_expr
from spinta import commands
from spinta.renderer import render
from spinta.components import Context, Action, UrlParams
from spinta.backends import log_getall
from spinta.datasets.components import ExternalBackend
from spinta.datasets.components import Entity


@commands.getall.register()
async def getall(
    context: Context,
    request: Request,
    entity: Entity,
    backend: ExternalBackend,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, entity)
    expr = urlparams_to_expr(params)
    rows = commands.getall(context, entity, backend, query=expr)
    hidden_props = [prop.name for prop in entity.model.properties.values() if prop.hidden]
    rows = log_getall(context, rows, select=params.select, hidden=hidden_props)
    result = (
        commands.prepare_data_for_response(
            context,
            action,
            entity.model,
            backend,
            row,
            select=params.select,
        )
        for row in rows
    )
    return render(context, request, entity.model, params, result, action=action)
