import csv

from spinta.commands import Command


class CsvModel(Command):
    metadata = {
        'name': 'csv',
        'type': 'dataset.model',
    }

    def execute(self):
        return self.read_csv()

    def read_csv(self):
        http = self.store.components.get('protocols.http')
        source = self.args.source.format(**self.args.dependency)
        with http.open(source, text=True) as f:
            yield from csv.DictReader(f)


class CsvDatasetProperty(Command):
    metadata = {
        'name': 'csv',
        'type': 'dataset.property',
    }

    def execute(self):
        return self.args.value.get(self.args.source)
