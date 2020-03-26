import itertools
import operator
import subprocess
import sys

from typing import Optional

from starlette.requests import Request
from starlette.responses import StreamingResponse

from texttable import Texttable

from spinta.commands.formats import Format
from spinta.utils.nestedstruct import flatten
from spinta.components import Context, Action, UrlParams, Model
from spinta import commands
from spinta.utils.response import aiter


class Ascii(Format):
    content_type = 'text/plain'
    accept_types = {
        'text/plain',
    }
    params = {
        'width': {'type': 'integer'},
        'colwidth': {'type': 'integer'},
    }

    def __call__(self, data, width=None, colwidth=42):
        data = iter(data)
        peek = next(data, None)

        if peek is None:
            return

        data = itertools.chain([peek], data)

        if '_type' in peek:
            groups = itertools.groupby(data, operator.itemgetter('_type'))
        else:
            groups = [(None, data)]

        for name, group in groups:
            if name:
                yield f'\n\nTable: {name}\n'

            rows = flatten(group)
            peek = next(rows, None)
            rows = itertools.chain([peek], rows)

            if name:
                cols = [k for k in peek.keys() if k != '_type']
            else:
                cols = list(peek.keys())

            if colwidth:
                width = len(cols) * colwidth

            buffer = [cols]
            tnum = 1

            for row in rows:
                buffer.append([row.get(c) for c in cols])
                if len(buffer) > 100:
                    yield from _draw(buffer, name, tnum, width)
                    buffer = [cols]
                    tnum += 1

            if buffer:
                yield from _draw(buffer, name, tnum, width)


@commands.render.register()  # noqa
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
    return _render(fmt, params, data, status_code, headers)


def _render(fmt: Ascii, params: UrlParams, data, status_code, headers):
    # Format params ar given in RQL query `?format(width(1),colwidth(1))`.
    width = params.formatparams.get('width')
    colwidth = params.formatparams.get('colwidth')

    if width is None and colwidth is None and sys.stdin.isatty():
        _, width = map(int, subprocess.run(['stty', 'size'], capture_output=True).stdout.split())
    elif width is None and colwidth is None:
        colwidth = 42

    return StreamingResponse(
        aiter(fmt(data, width, colwidth)),
        status_code=status_code,
        media_type=fmt.content_type,
        headers=headers,
    )


def _draw(buffer, name, tnum, width):
    if tnum > 1:
        if name:
            yield f"\n\nTable {name} #{tnum}:\n"
        else:
            yield f"\n\nTable #{tnum}:\n"

    table = Texttable(width)
    table.set_deco(Texttable.HEADER)
    table.add_rows(buffer)
    yield table.draw()
