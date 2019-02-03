from spinta.types import Type, LoadType


class Object(Type):
    metadata = {
        'name': 'object',
        'properties': {
            'properties': {'type': 'object'},
        },
    }


class Model(Object):
    metadata = {
        'name': 'model',
    }


class LoadObject(LoadType):
    name = 'load'
    types = ('object',)

    def execute(self, data):
        data = super().execute(data)
        if not isinstance(data['properties'], dict):
            data['properties'] = {}
        for name, prop in data['properties'].items():
            data['properties'][name] = self.manifest.load(prop)
        return data


class LoadManifestObject(LoadObject):
    name = 'load'
    types = ('model', 'dataset', 'project', 'owner')

    def execute(self, data):
        data = super().execute(data)

        if data['type'] not in self.manifest.objects:
            self.manifest.objects[data['type']] = {}

        if data['name'] in self.manifest.objects[data['type']]:
            raise Exception(f"Object {self.obj['type']} with name {self.obj['name']} already exist.")

        self.manifest.objects[data['type']][data['name']] = data
        return data
