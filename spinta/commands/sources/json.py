import json

from spinta.commands import Command


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
            http = self.store.components.get('protocols.http')
            with http.open(url) as f:
                data = json.load(f)
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
