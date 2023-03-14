import subprocess
import sys

from typing import Optional

from starlette.requests import Request
from starlette.responses import StreamingResponse

from spinta.formats.ascii.components import Ascii
from spinta.components import Context, Action, UrlParams, Model
from spinta import commands
from spinta.utils.response import aiter, peek_and_stream


@commands.render.register(Context, Request, Model, Ascii)
def render(
    context: Context,
    request: Request,
    model: Model,
    fmt: Ascii,
    *,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
    headers: Optional[dict] = None,
):
    return _render(
        context,
        model,
        fmt,
        action,
        params,
        data,
        status_code,
        headers,
    )


def _render(
    context: Context,
    model: Model,
    fmt: Ascii,
    action: Action,
    params: UrlParams,
    data,
    status_code,
    headers,
):
    # Format params ar given in RQL query `?format(width(1),max_col_width(1))`.
    width = params.formatparams.get('width')
    max_col_width = params.formatparams.get('colwidth')
    max_value_length = params.formatparams.get('vlen', 100)

    if width is None and max_col_width is None and sys.stdin.isatty():
        proc = subprocess.run(['stty', 'size'], capture_output=True)
        _, width = map(int, proc.stdout.split())
    elif width is None and max_col_width is None:
        max_col_width = 42

    return StreamingResponse(
        aiter(peek_and_stream(fmt(
            context,
            model,
            action,
            params,
            data,
            width,
            max_col_width,
            max_value_length,
        ))),
        status_code=status_code,
        media_type=fmt.content_type,
        headers=headers,
    )
