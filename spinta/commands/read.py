from starlette.requests import Request

from spinta import commands
from spinta.backends import get_select_prop_names
from spinta.backends import get_select_tree
from spinta.compat import urlparams_to_expr
from spinta.components import Context, Node, Action, UrlParams
from spinta.components import Model
from spinta.datasets.components import ExternalBackend
from spinta.renderer import render


@commands.getall.register(Context, Model, Request)
async def getall(
    context: Context,
    model: Model,
    request: Request,
    *,
    action: Action,
    params: UrlParams,
):
    commands.authorize(context, action, model)

    backend = model.backend

    if isinstance(backend, ExternalBackend):
        # XXX: `add_count` is a hack, because, external backends do not
        #      supports it yet.
        expr = urlparams_to_expr(params, add_count=False)
    else:
        expr = urlparams_to_expr(params)

    accesslog = context.get('accesslog')
    accesslog.log(
        model=model.model_type(),
        action=action.value,
    )

    rows = commands.getall(context, model, backend, query=expr)
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


@commands.getone.register(Context, Request, Node)
async def getone(
    context: Context,
    request: Request,
    node: Node,
    *,
    action: Action,
    params: UrlParams,
):
    if params.prop and params.propref:
        return await commands.getone(context, request, params.prop, params.model.backend, action=action, params=params)
    elif params.prop:
        return await commands.getone(context, request, params.prop, params.prop.dtype.backend or params.model.backend, action=action, params=params)
    else:
        return await commands.getone(context, request, params.model, params.model.backend, action=action, params=params)


@commands.changes.register()
async def changes(
    context: Context,
    request: Request,
    node: Node,
    *,
    action: Action,
    params: UrlParams,
):
    return await commands.changes(context, request, node, node.backend, action=action, params=params)
