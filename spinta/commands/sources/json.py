import json

from spinta.dispatcher import command


@command()
def read_json():
    pass

@read_json.register()
def _(context: Context, model: Model, *, source=None, dependency=None, items=None):
    session = context.get('pull.session')
    urls = source if isinstance(source, list) else [source]
    for url in urls:
        url = url.format(**dependency)
        with session.open(url) as f:
            data = json.load(f)
        data = data[self.args.items]
        if isinstance(data, list):
            yield from data
        else:
            yield data


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


class JsonDatasetProperty(Command):
    metadata = {
        'name': 'json',
        'type': 'dataset.property',
    }

    def execute(self):
        return self.args.value.get(self.args.source)
