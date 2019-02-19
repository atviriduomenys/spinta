from spinta.commands import Command


class XmlDataset(Command):
    metadata = {
        'name': 'xlsx',
        'type': 'dataset.model',
    }

    def execute(self):
        return None
