from spinta.components import Node


class Dimension(Node):
    pass


class UriPrefix(Dimension):
    type: str = 'prefix'
    id: str = None
    eid: str = None
    name: str = None
    uri: str = None
    title: str = None
    description: str = None

    schema = {
        'id': {'type': 'string'},
        'eid': {'type': 'string'},
        'type': {'type': 'string'},
        'name': {'type': 'string'},
        'uri': {'type': 'string'},
        'title': {'type': 'string'},
        'description': {'type': 'string'},
    }

    def __repr__(self):
        return f"<{type(self).__name__} name={self.name!r}>"
