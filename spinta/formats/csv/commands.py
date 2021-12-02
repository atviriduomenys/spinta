from typing import Dict

from starlette.requests import Request
from starlette.responses import Response
from starlette.responses import StreamingResponse

from spinta.formats.csv.components import Csv
from spinta.components import Context, Action, UrlParams, Model
from spinta import commands
from spinta.utils.response import aiter


@commands.render.register(Context, Request, Model, Csv)
def render(
    context: Context,
    request: Request,
    model: Model,
    fmt: Csv,
    *,
    action: Action,
    params: UrlParams,
    data,
    status_code: int = 200,
    headers: Dict[str, str] = None,
) -> Response:
    headers = headers or {}
    headers['Content-Disposition'] = f'attachment; filename="{model.basename}.csv"'
    return StreamingResponse(
        aiter(fmt(data)),
        status_code=status_code,
        media_type=fmt.content_type,
        headers=headers,
    )
