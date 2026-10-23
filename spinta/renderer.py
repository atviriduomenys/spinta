from collections.abc import Iterator
from typing import Any

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
    action: Action | None = None,
    status_code: int | None = 200,
    headers: dict | None = None,
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
