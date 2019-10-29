from typing import Optional

from starlette.requests import Request

from spinta.components import Context, Action, UrlParams, Node
from spinta import commands


def render(
    context: Context,
    request: Request,
    node: Node,
    params: UrlParams,
    data,
    *,
    action: Optional[Action] = None,
    status_code: int = 200,
):
    config = context.get('config')
    fmt = config.exporters[params.format]
    return commands.render(context, request, node, fmt, action=action, params=params, data=data, status_code=status_code)
