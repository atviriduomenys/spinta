import io
import pathlib
from typing import Any
from typing import Dict
from typing import Iterator

from starlette.requests import Request
from starlette.responses import Response
from starlette.responses import StreamingResponse

from spinta import commands
from spinta.components import Action
from spinta.components import Context
from spinta.components import Model
from spinta.components import UrlParams
from spinta.formats.helpers import get_model_tabular_header
from spinta.formats.xlsx.components import Xlsx
from spinta.manifests.components import Manifest
from spinta.manifests.tabular.components import ManifestRow
from spinta.manifests.tabular.constants import DATASET
from spinta.manifests.tabular.helpers import datasets_to_tabular, write_xlsx
from spinta.utils.nestedstruct import flatten, sepgetter
from spinta.utils.response import aiter


@commands.render.register(Context, Request, Model, Xlsx)
def render(
    context: Context,
    request: Request,
    model: Model,
    fmt: Xlsx,
    *,
    action: Action,
    params: UrlParams,
    data: Iterator[Dict[str, Any]],
    status_code: int = 200,
    headers: Dict[str, str] = None,
) -> Response:
    headers = headers or {}
    headers['Content-Disposition'] = f'attachment; filename="{model.basename}.xlsx"'

    rows = flatten(data, sepgetter(model))
    cols = get_model_tabular_header(context, model, action, params)
    return StreamingResponse(
        aiter(_render_xlsx(rows, cols)),
        status_code=status_code,
        media_type=fmt.content_type,
        headers=headers,
    )


@commands.render.register(Context, Manifest, Xlsx)
def render(
    context: Context,
    manifest: Manifest,
    fmt: Xlsx,
    *,
    action: Action = None,
    params: UrlParams = None,
    status_code: int = 200,
    headers: Dict[str, str] = None,
    path: str = None
) -> Response:
    rows = datasets_to_tabular(context, manifest)
    rows = ({c: row[c] for c in DATASET} for row in rows)
    if not path:
        headers = headers or {}
        headers['Content-Disposition'] = f'attachment; filename="{manifest.name}.xlsx"'
        return StreamingResponse(
            aiter(_render_xlsx(rows, DATASET)),
            status_code=status_code,
            media_type=fmt.content_type,
            headers=headers,
        )
    else:
        write_xlsx(pathlib.Path(path), rows, DATASET)


def _render_xlsx(
    rows: Iterator[ManifestRow],
    cols: list
):
    stream = io.BytesIO()
    write_xlsx(stream, rows, cols)
    stream.seek(0)
    for row in stream.readlines():
        yield row
