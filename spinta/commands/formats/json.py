import ujson as json


class Json:
    content_type = 'application/json'
    params = {
        'wrap': {'type': 'boolean'},
    }

    def __call__(self, rows, *, wrap: bool = True):
        if wrap:
            yield '{"data":['
        for i, row in enumerate(rows):
            yield (',' if i > 0 else '') + json.dumps(row, ensure_ascii=False)
        if wrap:
            yield ']}'
