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
        },
    }


class ManifestCheckModel(Command):
    metadata = {
        'name': 'manifest.check',
        'type': 'model',
    }

    def execute(self):
        super().execute()
        primary_keys = []
        for prop in self.obj.properties.values():
            if prop.type == 'pk':
                primary_keys.append(prop)
        n_pkeys = len(primary_keys)
        if n_pkeys > 1:
            self.error(f"Only one primary key is allowed, found {n_pkeys}")
        elif n_pkeys == 0:
            self.error(f"At leas one primary key must be defined for model.")
