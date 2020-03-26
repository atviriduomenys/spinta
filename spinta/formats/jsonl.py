import ujson as json

from typing import Optional

from starlette.requests import Request
from starlette.responses import StreamingResponse

from spinta.commands.formats import Format
from spinta.components import Context, Action, UrlParams, Model, Node
from spinta import commands


class JsonLines(Format):
    content_type = 'application/x-json-stream'
    accept_types = {
        'application/x-json-stream',
    }
    params = {}

    def __call__(self, data):
        for row in data:
            yield json.dumps(row, ensure_ascii=False) + '\n'


@commands.render.register()  # noqa
def render(
    context: Context,
    request: Request,
    model: Model,
    fmt: JsonLines,
    *,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
    headers: Optional[dict] = None,
):
    return _render(fmt, data, status_code, headers)


@commands.render.register()  # noqa
def render(  # noqa
    context: Context,
    request: Request,
    node: Node,
    fmt: JsonLines,
    *,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
    headers: Optional[dict] = None,
):
    return _render(fmt, data, status_code, headers)


def _render(fmt: JsonLines, data, status_code: int, headers: dict):
    stream = _stream(data)
    return StreamingResponse(
        stream,
        status_code=status_code,
        media_type=fmt.content_type,
        headers=headers
    )


async def _stream(data):
    for row in data:
        yield json.dumps(row, ensure_ascii=False) + '\n'
