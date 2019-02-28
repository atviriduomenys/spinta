import ujson as json

from spinta.commands import Command


class IterableFile:

    def __init__(self):
        self.writes = []

    def __iter__(self):
        yield from self.writes
        self.writes = []

    def write(self, data):
        self.writes.append(data)


class Csv(Command):
    metadata = {
        'name': 'export.json',
        'type': 'dataset.model',
    }

    def execute(self):
        yield '{"data":['
        for i, row in enumerate(self.args.rows):
            yield (',' if i > 0 else '') + json.dumps(row)
        yield ']}'
