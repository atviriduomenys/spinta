import csv
from typing import Any
from typing import Dict
from typing import Iterator

import itertools
from starlette.requests import Request
from starlette.responses import Response
from starlette.responses import StreamingResponse

from spinta import commands
from spinta.components import Action
from spinta.components import Context
from spinta.components import Model
from spinta.components import UrlParams
from spinta.formats.csv.components import Csv
from spinta.formats.csv.components import IterableFile
from spinta.formats.helpers import get_model_tabular_header
from spinta.utils.nestedstruct import flatten
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
    data: Iterator[Dict[str, Any]],
    status_code: int = 200,
    headers: Dict[str, str] = None,
) -> Response:
    headers = headers or {}
    headers['Content-Disposition'] = f'attachment; filename="{model.basename}.csv"'
    return StreamingResponse(
        aiter(_render_csv(context, model, action, params, data)),
        status_code=status_code,
        media_type=fmt.content_type,
        headers=headers,
    )


def _render_csv(
    context: Context,
    model: Model,
    action: Action,
    params: UrlParams,
    data: Iterator[Dict[str, Any]],
):
    rows = flatten(data)
    cols = get_model_tabular_header(context, model, action, params)

    stream = IterableFile()
    writer = csv.DictWriter(stream, fieldnames=cols)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
        yield from stream
