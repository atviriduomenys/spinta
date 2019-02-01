from spinta.types import Type, NA, LoadObject


class Object(Type):
    name = 'object'

    def resolve(self):
        with self.ctx.push('properties', self.ctx.schema.get('properties', NA), self.ctx.source, {}):
            self.ctx.run({'required': True})
            self.ctx.run({'type': 'object'})
            for name, prop in self.ctx.schema.items():
                with self.ctx.push(name, prop, self.ctx.source.get(name, NA), NA):
                    self.ctx.stack[-2].output = self.ctx.run({'resolve': None})


class Model(Object):
    name = 'model'

    functions = {
        'reflect': {'title': "Make sure model matches database schema"},
        'validate': {'title': "Validate data"},
        'unique': {'title': "Check if lists of fields are unique"},
    }


class LoadManifestObject(LoadObject):
    name = 'load'
    types = ('model', 'dataset', 'project', 'owner')

    def execute(self, schema):
        obj = super().execute(schema)

        if obj['type'] not in self.manifest.objects:
            self.manifest.objects[obj['type']] = {}

        if obj['name'] in self.manifest.objects[obj['type']]:
            raise Exception(f"Object {obj['type']} with name {obj['name']} already exist.")

        self.manifest.objects[obj['type']][obj['name']] = obj
