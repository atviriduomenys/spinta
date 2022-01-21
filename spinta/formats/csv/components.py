from spinta.formats.components import Format


class IterableFile:

    def __init__(self):
        self.writes = []

    def __iter__(self):
        yield from self.writes
        self.writes = []

    def write(self, data):
        self.writes.append(data)


class Csv(Format):
    content_type = 'text/csv'
    accept_types = {
        'text/csv',
    }
    params = {}
