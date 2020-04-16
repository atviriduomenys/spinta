from starlette.requests import Request

from spinta import commands
from spinta.components import Context, Node, Action, UrlParams


@commands.getall.register(Context, Request, Node)
async def getall(
    context: Context,
    request: Request,
    node: Node,
    *,
    action: Action,
    params: UrlParams,
):
    return await commands.getall(context, request, node, node.backend, action=action, params=params)


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
