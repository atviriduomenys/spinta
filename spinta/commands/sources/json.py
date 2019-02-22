from spinta.commands import Command

import requests


class JsonModel(Command):
    metadata = {
        'name': 'json',
        'type': 'dataset.model',
        'compose': 'getitem',
        'argument': 'source',
        'arguments': {
            'source': {'type': 'url', 'required': True},
        }
    }

    def execute(self):
        with requests.get(self.args.source) as r:
            data = r.json()
            if isinstance(data, list):
                yield from data
            else:
                yield data


class GetItemModel(Command):
    metadata = {
        'name': 'getitem',
        'type': 'dataset.model',
        'argument': 'name',
        'arguments': {
            'name': {'type': 'string', 'required': True},
        }
    }

    def execute(self):
        for row in self.value:
            yield from row.get(self.args.name, [])


class GetItem(Command):
    metadata = {
        'name': 'getitem',
        'argument': 'name',
        'arguments': {
            'name': {'type': 'string', 'required': True},
        }
    }

    def execute(self):
        return self.value.get(self.args.name)
