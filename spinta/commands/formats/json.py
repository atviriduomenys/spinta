import ujson as json

from spinta.commands import Command


class Json(Command):
    metadata = {
        'name': 'export.json',
    }

    def execute(self):
        if self.args.wrap:
            yield '{"data":['
        for i, row in enumerate(self.args.rows):
            yield (',' if i > 0 else '') + json.dumps(row, ensure_ascii=False)
        if self.args.wrap:
            yield ']}'
