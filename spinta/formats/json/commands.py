from typing import Optional

from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse

from spinta.components import Context, Action, UrlParams, Node
from spinta import commands
from spinta.formats.json.components import Json
from spinta.utils.response import aiter, peek_and_stream


@commands.render.register(Context, Request, Node, Json)
def render(
    context: Context,
    request: Request,
    node: Node,
    fmt: Json,
    *,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
    headers: Optional[dict] = None,
):
    return _render(
        context,
        request,
        fmt,
        action,
        params,
        data,
        status_code,
        headers,
    )


def _render(
    context: Context,
    request: Request,
    fmt: Json,
    action: Action,
    params: UrlParams,
    data,
    status_code: int,
    headers: dict,
):
    if action in (Action.GETALL, Action.SEARCH, Action.CHANGES):
        # In python dict is also an iterable, but here we want a true iterable,
        # a list or a generator of dicts.
        assert not isinstance(data, dict), data
        return StreamingResponse(
            aiter(peek_and_stream(fmt(data))),
            status_code=status_code,
            media_type=fmt.content_type,
            headers=headers,
        )
    elif action == Action.DELETE:
        return Response(None, status_code=status_code, headers=headers)
    else:
        return JSONResponse(
            fmt.data(data),
            status_code=status_code,
            headers=headers
        )
