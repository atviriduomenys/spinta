import ujson as json

from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.responses import StreamingResponse

from spinta.commands.formats import Format
from spinta.components import Context, Action, UrlParams, Node
from spinta import commands
from spinta.utils.response import aiter, peek_and_stream


class Json(Format):
    content_type = 'application/json'
    accept_types = {
        'application/json',
    }
    params = {}
    container_name = '_data'

    def __call__(self, data):
        yield f'{{"{self.container_name}":['
        for i, row in enumerate(data):
            sep = ',' if i > 0 else ''
            yield sep + json.dumps(row, ensure_ascii=False)
        yield ']}'


@commands.render.register()  # noqa
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
):
    return _render(context, request, fmt, action, params, data, status_code)


def _render(
    context: Context,
    request: Request,
    fmt: Json,
    action: Action,
    params: UrlParams,
    data,
    status_code: int,
):
    if action in (Action.GETALL, Action.SEARCH, Action.CHANGES):
        # In python dict is also an iterable, but here we want a true iterable,
        # a list or a generator of dicts.
        assert not isinstance(data, dict), data
        return StreamingResponse(
            aiter(peek_and_stream(fmt(data))),
            status_code=status_code,
            media_type=fmt.content_type,
        )

    else:
        return JSONResponse(data, status_code=status_code)
