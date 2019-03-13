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
        return self.properties['id']


class CheckModel(Command):
    metadata = {
        'name': 'manifest.check',
        'type': 'model',
    }

    def execute(self):
        super().execute()
        self.check_primary_key()

    def check_primary_key(self):
        if 'id' not in self.obj.properties:
            self.error("Primary key is required, add `id` property to the model.")
        if self.obj.properties['id'].type == 'pk':
            self.deprecation("`id` property must specify real type like 'string' or 'integer'. Use of 'pk' is deprecated.")
