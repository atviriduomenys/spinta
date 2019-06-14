import ujson as json

from spinta.commands.formats import Format


class JsonLines(Format):
    content_type = 'application/x-json-stream'
    accept_types = {
        'application/x-json-stream',
    }
    params = {}

    def __call__(self, rows):
        for i, row in enumerate(rows):
            yield json.dumps(row, ensure_ascii=False) + '\n'
