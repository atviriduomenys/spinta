import operator

import itertools

from spinta.formats.components import Format
from spinta.formats.ascii.helpers import draw
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
                    yield from draw(buffer, name, tnum, width)
                    buffer = [cols]
                    tnum += 1

            if buffer:
                yield from draw(buffer, name, tnum, width)
