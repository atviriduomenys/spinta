import itertools
import operator
import subprocess
import sys

from texttable import Texttable

from spinta.commands.formats import Format
from spinta.utils.nestedstruct import flatten


class Ascii(Format):
    content_type = 'text/plain'
    accept_types = {
        'text/plain',
    }
    params = {
        'width': {'type': 'integer'},
        'colwidth': {'type': 'integer'},
    }

    def __call__(self, rows, *, width: int = None, colwidth: int = None):
        rows = iter(rows)
        peek = next(rows, None)

        if peek is None:
            return

        rows = itertools.chain([peek], rows)

        if width is None and colwidth is None and sys.stdin.isatty():
            _, width = map(int, subprocess.run(['stty', 'size'], capture_output=True).stdout.split())
        elif width is None and colwidth is None:
            colwidth = 42

        if 'type' in peek:
            groups = itertools.groupby(rows, operator.itemgetter('type'))
        else:
            groups = [(None, rows)]

        for name, group in groups:
            if name:
                yield f'\n\nTable: {name}\n'

            rows = flatten(group)
            peek = next(rows, None)
            rows = itertools.chain([peek], rows)

            if name:
                cols = [k for k in peek.keys() if k != 'type']
            else:
                cols = list(peek.keys())

            buffer = [cols]
            tnum = 1

            if colwidth:
                width = len(cols) * colwidth

            for row in rows:
                buffer.append([row.get(c) for c in cols])
                if len(buffer) > 100:
                    yield from _draw(buffer, name, tnum, width)
                    buffer = [cols]
                    tnum += 1

            if buffer:
                yield from _draw(buffer, name, tnum, width)


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
