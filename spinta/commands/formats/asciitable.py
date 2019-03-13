import itertools
import subprocess
import sys

from texttable import Texttable

from spinta.commands import Command
from spinta.utils.nestedstruct import flatten


class AsciiTable(Command):
    metadata = {
        'name': 'export.asciitable',
        'type': 'dataset.model',
    }

    def execute(self):
        rows = flatten(self.args.rows)
        peek = next(rows, None)

        if peek is None:
            return

        cols = list(peek.keys())
        rows = itertools.chain([peek], rows)
        buffer = [cols]
        tnum = 1

        if 'column_width' in self.args.args:
            width = len(cols) * self.args.column_width
        elif 'width' in self.args.args:
            width = self.args.width
        elif sys.stdin.isatty():
            _, width = map(int, subprocess.run(['stty', 'size'], capture_output=True).stdout.split())
        else:
            width = 80

        for row in rows:
            buffer.append([row.get(c) for c in cols])
            if len(buffer) > 100:
                yield from self.draw(buffer, tnum, width)
                buffer = [cols]
                tnum += 1

        if buffer:
            yield from self.draw(buffer, tnum, width)

    def draw(self, buffer, tnum, width):
        if tnum > 1:
            yield f"\n\nTable #{tnum}:\n"

        table = Texttable(width)
        table.set_deco(Texttable.HEADER)
        table.add_rows(buffer)
        yield table.draw()
