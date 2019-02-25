from spinta.commands import Command

import requests


class Json(Command):
    metadata = {
        'name': 'json',
        'type': 'dataset.model',
        'arguments': {
            'source': {'type': 'url', 'required': True},
        }
    }

    def execute(self):
        with requests.get(self.args.url) as r:
            data = r.json()
            data = data[self.args.items]
            if isinstance(data, list):
                yield from data
            else:
                yield data
