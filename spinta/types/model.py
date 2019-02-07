from spinta.types.object import Object, ManifestLoadObject


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


class ManifestLoadModel(ManifestLoadObject):
    metadata = {
        'name': 'manifest.load',
        'type': ('model', 'dataset', 'project', 'owner'),
    }

    def execute(self, data):
        super().execute(data)

        if self.obj.type not in self.store.objects[self.ns]:
            self.store.objects[self.ns][self.obj.type] = {}

        if self.obj.name in self.store.objects[self.ns][self.obj.type]:
            raise Exception(f"Object {self.obj.type} with name {self.obj.name} already exist.")

        self.store.objects[self.ns][self.obj.type][self.obj.name] = self.obj
