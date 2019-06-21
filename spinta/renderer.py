from starlette.requests import Request

from spinta.components import Context, Action, UrlParams, Node
from spinta import commands


def render(
    context: Context,
    request: Request,
    node: Node,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
):
    config = context.get('config')
    fmt = config.exporters[params.format]

    if node:
        return commands.render(context, request, node, fmt, action=action, params=params, data=data, status_code=status_code)
    else:
        return commands.render(context, request, fmt, action=action, params=params, data=data, status_code=status_code)
