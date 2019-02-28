import csv

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
        'name': 'export.csv',
        'type': 'dataset.model',
    }

    def execute(self):
        stream = IterableFile()
        writer = csv.DictWriter(stream, fieldnames=self.args.cols)
        writer.writeheader()
        for row in self.args.rows:
            writer.writerow(row)
            yield from stream
