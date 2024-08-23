import csv
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
from spinta.formats.csv.components import Csv
from spinta.formats.csv.components import IterableFile
from spinta.formats.helpers import get_model_tabular_header, rename_page_col
from spinta.manifests.components import Manifest
from spinta.manifests.tabular.components import ManifestRow
from spinta.manifests.tabular.constants import DATASET
from spinta.manifests.tabular.helpers import datasets_to_tabular, write_csv
from spinta.utils.nestedstruct import flatten, sepgetter
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
        aiter(_render_model_csv(context, model, action, params, data)),
        status_code=status_code,
        media_type=fmt.content_type,
        headers=headers,
    )


@commands.render.register(Context, Manifest, Csv)
def render(
    context: Context,
    manifest: Manifest,
    fmt: Csv,
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
        headers['Content-Disposition'] = f'attachment; filename="{manifest.name}.csv"'
        return StreamingResponse(
            aiter(_render_manifest_csv(rows)),
            status_code=status_code,
            media_type=fmt.content_type,
            headers=headers,
        )
    else:
        write_csv(pathlib.Path(path), rows, DATASET)


def _render_manifest_csv(
    rows: Iterator[ManifestRow]
):
    stream = IterableFile()
    writer = csv.DictWriter(stream, fieldnames=DATASET)
    writer.writeheader()
    writer.writerows(rows)
    yield from stream


def _render_model_csv(
    context: Context,
    model: Model,
    action: Action,
    params: UrlParams,
    data: Iterator[Dict[str, Any]],
):
    rows = flatten(data, sepgetter(model))
    # Rename _page to _page.next
    rows = rename_page_col(rows)
    cols = get_model_tabular_header(context, model, action, params)
    cols = [col if col != '_page' else '_page.next' for col in cols]
    stream = IterableFile()
    writer = csv.DictWriter(stream, fieldnames=cols)
    writer.writeheader()
    memory = next(rows, None)
    for row in rows:
        writer.writerow({k: v for k, v in memory.items() if k != '_page.next'})
        yield from stream
        memory = row

    if memory is not None:
        writer.writerow(memory)
    yield from stream
