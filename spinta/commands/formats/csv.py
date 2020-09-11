import csv
import itertools
from typing import Dict

from typing import Optional

from starlette.requests import Request
from starlette.responses import StreamingResponse

from spinta.commands.formats import Format
from spinta.utils.nestedstruct import flatten
from spinta.components import Context, Action, UrlParams, Model
from spinta import commands
from spinta.utils.response import aiter


class IterableFile:

    def __init__(self):
        self.writes = []

    def __iter__(self):
        yield from self.writes
        self.writes = []

    def write(self, data):
        self.writes.append(data)


class Csv(Format):
    content_type = 'text/csv'
    accept_types = {
        'text/csv',
    }
    params = {}

    def __call__(self, data):
        rows = flatten(data)
        peek = next(rows, None)

        if peek is None:
            return

        cols = list(peek.keys())
        rows = itertools.chain([peek], rows)

        stream = IterableFile()
        writer = csv.DictWriter(stream, fieldnames=cols)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            yield from stream


@commands.render.register()
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
):
    headers = headers or {}
    headers['Content-Disposition'] = f'attachment; filename="{model.basename}.csv"'
    return StreamingResponse(
        aiter(fmt(data)),
        status_code=status_code,
        media_type=fmt.content_type,
        headers=headers,
    )
