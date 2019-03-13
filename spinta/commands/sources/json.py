from spinta.commands import Command

import requests


class Json(Command):
    metadata = {
        'name': 'json',
        'type': 'dataset.model',
        'arguments': {
            'source': {'type': 'url', 'required': True},
            'items': {'type': 'string', 'required': True},
        }
    }

    def execute(self):
        urls = self.args.url if isinstance(self.args.url, list) else [self.args.url]
        for url in urls:
            url = url.format(**self.args.dependency)
            with requests.get(url) as r:
                data = r.json()
                data = data[self.args.items]
                if isinstance(data, list):
                    yield from data
                else:
                    yield data


class JsonDatasetProperty(Command):
    metadata = {
        'name': 'json',
        'type': 'dataset.property',
    }

    def execute(self):
        return self.args.value.get(self.args.source)
