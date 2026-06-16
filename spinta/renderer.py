from typing import Any, Iterator, Optional

from starlette.requests import Request
from starlette.responses import Response

from spinta import commands
from spinta.components import Context, Node, UrlParams
from spinta.core.enums import Action


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
        context,
        request,
        node,
        params.fmt,
        action=action,
        params=params,
        data=data,
        status_code=status_code,
        headers=headers,
    )
