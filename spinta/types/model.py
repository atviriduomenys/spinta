from spinta.types.object import Object, LoadObject


class Model(Object):
    metadata = {
        'name': 'model',
        'properties': {
            'path': {'type': 'path', 'required': True},
            'unique': {'type': 'array', 'default': []},
            'extends': {'type': 'string'},
            'backend': {'type': 'string', 'default': 'default'},
        },
    }


class LoadManifestObject(LoadObject):
    name = 'manifest.load'
    types = ('model', 'dataset', 'project', 'owner')

    def execute(self, data):
        super().execute(data)

        if self.schema.type not in self.manifest.objects:
            self.manifest.objects[self.schema.type] = {}

        if self.schema.name in self.manifest.objects[self.schema.type]:
            raise Exception(f"Object {self.schema.type} with name {self.schema.name} already exist.")

        self.manifest.objects[self.schema.type][self.schema.name] = self.schema
