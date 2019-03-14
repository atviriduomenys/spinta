import ujson as json

from spinta.commands import Command


class JsonLines(Command):
    metadata = {
        'name': 'export.jsonl',
    }

    def execute(self):
        for i, row in enumerate(self.args.rows):
            yield json.dumps(row, ensure_ascii=False) + '\n'
