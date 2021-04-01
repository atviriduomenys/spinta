from starlette.requests import Request

from spinta.backends import get_select_prop_names
from spinta.backends import get_select_tree
from spinta.compat import urlparams_to_expr
from spinta import commands
from spinta.renderer import render
from spinta.components import Context, Action, UrlParams
from spinta.components import Model
from spinta.backends import log_getall
from spinta.datasets.components import ExternalBackend


@commands.getall.register(Context, Request, Model, ExternalBackend)
async def getall(
    context: Context,
    request: Request,
    model: Model,
    backend: ExternalBackend,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, model)
    expr = urlparams_to_expr(params, add_count=False)
    rows = commands.getall(context, model, backend, query=expr)
    hidden_props = [prop.name for prop in model.properties.values() if prop.hidden]
    rows = log_getall(context, rows, select=params.select, hidden=hidden_props)
    if not params.count:
        select_tree = get_select_tree(context, action, params.select)
        prop_names = get_select_prop_names(context, model, action, select_tree)
        rows = (
            commands.prepare_data_for_response(
                context,
                action,
                model,
                backend,
                row,
                select=select_tree,
                prop_names=prop_names,
            )
            for row in rows
        )
    return render(context, request, model, params, rows, action=action)
