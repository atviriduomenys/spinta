from typing import Any
from typing import Iterator
from typing import Optional

from spinta.components import Context, Action, UrlParams, Node
from spinta import commands
from spinta.utils.imports import use

Request = use('http', 'starlette.requests', 'Request')
Response = use('http', 'starlette.responses', 'Response')


def render(
    context: Context,
    request: Request,
    node: Node,
    params: UrlParams,
    data: Iterator[Any],
    *,
    action: Optional[Action] = None,
    status_code: Optional[int] = 200,
    headers: Optional[dict] = None,
) -> Response:
    return commands.render(
        context, request, node, params.fmt,
        action=action,
        params=params,
        data=data,
        status_code=status_code,
        headers=headers,
    )
