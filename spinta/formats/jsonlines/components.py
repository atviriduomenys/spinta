import ujson as json

from spinta.formats.components import Format


class JsonLines(Format):
    content_type = 'application/x-json-stream'
    accept_types = {
        'application/x-json-stream',
    }
    params = {}

    def __call__(self, data):
        for row in data:
            yield json.dumps(row, ensure_ascii=False) + '\n'
