import ujson as json

from starlette.requests import Request
from starlette.responses import StreamingResponse

from spinta.commands.formats import Format
from spinta.components import Context, Action, UrlParams, Model
from spinta import commands
from spinta.types import dataset


class JsonLines(Format):
    content_type = 'application/x-json-stream'
    accept_types = {
        'application/x-json-stream',
    }
    params = {}


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
):
    return _render(fmt, data, status_code)


@commands.render.register()  # noqa
def render(
    context: Context,
    request: Request,
    model: dataset.Model,
    fmt: JsonLines,
    *,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
):
    return _render(fmt, data, status_code)


def _render(fmt: JsonLines, data, status_code: int):
    stream = _stream(data)
    return StreamingResponse(stream, status_code=status_code, media_type=fmt.content_type)


async def _stream(data):
    for row in data:
        yield json.dumps(row, ensure_ascii=False) + '\n'
