from spinta.commands import Command
from spinta.types.object import Object


class Model(Object):
    metadata = {
        'name': 'model',
        'properties': {
            'path': {'type': 'path', 'required': True},
            'unique': {'type': 'array', 'default': []},
            'extends': {'type': 'string'},
            'backend': {'type': 'string', 'default': 'default'},
            'version': {'type': 'integer', 'required': True},
            'date': {'type': 'date', 'required': True},
            'link': {'type': 'string'},
            'parent': {'type': 'manifest'},
        },
    }

    def get_type_value(self):
        return self.name

    def get_primary_key(self):
        for prop in self.properties.values():
            if prop.type == 'pk':
                return prop
        raise Exception(f"{self} does not have a primary key.")


class CheckModel(Command):
    metadata = {
        'name': 'manifest.check',
        'type': 'model',
    }

    def execute(self):
        super().execute()
        self.check_primary_key()

    def check_primary_key(self):
        pkeys = {name for name, prop in self.obj.properties.items() if prop.type == 'pk'}
        n_pkeys = len(pkeys)
        if n_pkeys > 1:
            self.error("Only one primary key is allowed, found {n_pkeys}.")
        if n_pkeys == 0:
            self.error("Primary key is required, add `id: {type: pk}` to the model.")
        pkey = next(iter(pkeys))
        if pkey != 'id':
            self.error("Primary key must be named 'id', but a primary key named {pkey!r} is found. Change it to 'id'.")
