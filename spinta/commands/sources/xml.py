from spinta.commands import Command


class XmlDataset(Command):
    metadata = {
        'name': 'xml',
        'type': 'dataset',
    }

    def execute(self):
        return None
