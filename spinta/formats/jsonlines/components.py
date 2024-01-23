import ujson as json

from spinta.formats.components import Format


class JsonLines(Format):
    content_type = 'application/x-json-stream'
    accept_types = {
        'application/x-json-stream',
    }
    params = {}

    def __call__(self, data):
        memory = next(data)
        for row in data:
            yield json.dumps({k: v for k, v in memory.items() if k != '_page'}, ensure_ascii=False) + '\n'
            memory = row

        last_row = {}
        for k, v in memory.items():
            if k == '_page':
                last_row[k] = {
                    'next': v
                }
            else:
                last_row[k] = v
        yield json.dumps(last_row, ensure_ascii=False) + '\n'
