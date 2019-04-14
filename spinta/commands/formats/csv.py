import csv
import itertools

from spinta.utils.nestedstruct import flatten


class IterableFile:

    def __init__(self):
        self.writes = []

    def __iter__(self):
        yield from self.writes
        self.writes = []

    def write(self, data):
        self.writes.append(data)


class Csv:
    content_type = 'text/csv'
    params = {}

    def __call__(self, rows):
        rows = flatten(rows)
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
