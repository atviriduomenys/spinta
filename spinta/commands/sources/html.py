from spinta.commands import Command


class HtmlDataset(Command):
    metadata = {
        'name': 'html',
        'type': 'dataset',
    }

    def execute(self):
        return None
