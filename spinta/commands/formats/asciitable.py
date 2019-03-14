import itertools
import operator
import subprocess
import sys

from texttable import Texttable

from spinta.commands import Command
from spinta.utils.nestedstruct import flatten


class AsciiTable(Command):
    metadata = {
        'name': 'export.asciitable',
    }

    def execute(self):
        rows = iter(self.args.rows)
        peek = next(rows, None)

        if peek is None:
            return

        rows = itertools.chain([peek], rows)

        if 'column_width' in self.args.args:
            width = None
        elif 'width' in self.args.args:
            width = self.args.width
        elif sys.stdin.isatty():
            _, width = map(int, subprocess.run(['stty', 'size'], capture_output=True).stdout.split())
        else:
            width = 80

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

            if 'column_width' in self.args.args:
                width = len(cols) * self.args.column_width

            for row in rows:
                buffer.append([row.get(c) for c in cols])
                if len(buffer) > 100:
                    yield from self.draw(buffer, name, tnum, width)
                    buffer = [cols]
                    tnum += 1

            if buffer:
                yield from self.draw(buffer, name, tnum, width)

    def draw(self, buffer, name, tnum, width):
        if tnum > 1:
            if name:
                yield f"\n\nTable {name} #{tnum}:\n"
            else:
                yield f"\n\nTable #{tnum}:\n"

        table = Texttable(width)
        table.set_deco(Texttable.HEADER)
        table.add_rows(buffer)
        yield table.draw()
