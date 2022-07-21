import subprocess
import sys

from typing import Optional

from starlette.requests import Request
from starlette.responses import StreamingResponse

from spinta.formats.ascii.components import Ascii
from spinta.components import Context, Action, UrlParams, Model
from spinta import commands
from spinta.utils.response import aiter


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
    # Format params ar given in RQL query `?format(width(1),colwidth(1))`.
    width = params.formatparams.get('width')
    colwidth = params.formatparams.get('colwidth')

    if width is None and colwidth is None and sys.stdin.isatty():
        proc = subprocess.run(['stty', 'size'], capture_output=True)
        _, width = map(int, proc.stdout.split())
    elif width is None and colwidth is None:
        colwidth = 42

    return StreamingResponse(
        aiter(fmt(context, model, action, params, data, width, colwidth)),
        status_code=status_code,
        media_type=fmt.content_type,
        headers=headers,
    )
