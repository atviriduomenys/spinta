from spinta.commands import Command
from spinta.types import Type
from spinta.types.object import Object


class Dataset(Type):
    metadata = {
        'name': 'dataset',
        'properties': {
            'path': {'type': 'path', 'required': True},
            'source': {'type': 'command'},
            'objects': {'type': 'object', 'default': {}},
            'version': {'type': 'integer', 'required': True},
            'date': {'type': 'date', 'required': True},
            'owner': {'type': 'string'},
            'stars': {'type': 'integer'},
            'parent': {'type': 'manifest'},
        },
    }


class Model(Object):
    metadata = {
        'name': 'dataset.model',
        'properties': {
            'source': {'type': 'command'},
            'identity': {'type': 'array'},
            'properties': {'type': 'object', 'default': {}},
            'stars': {'type': 'integer'},
            'local': {'type': 'boolean'},
            'parent': {'type': 'dataset'},
        },
    }

    property_type = 'dataset.property'


class Property(Type):
    metadata = {
        'name': 'dataset.property',
        'properties': {
            'source': {'type': 'command'},
            'local': {'type': 'boolean'},
            'stars': {'type': 'integer'},
            'const': {'type': 'any'},
            'enum': {'type': 'array'},
            'parent': {'type': 'dataset.model'},
        },
    }


class LoadDataset(Command):
    metadata = {
        'name': 'manifest.load',
        'type': 'dataset',
    }

    def execute(self):
        super().execute()
        assert isinstance(self.obj.objects, dict)
        for name, obj in self.obj.objects.items():
            self.obj.objects[name] = self.load({
                'type': 'dataset.model',
                'name': name,
                'parent': self.obj,
                **(obj or {}),
            })


class PrepareDataset(Command):
    metadata = {
        'name': 'prepare.type',
        'type': 'dataset',
    }

    def execute(self):
        super().execute()
        for model in self.obj.objects.values():
            self.run(model, {'prepare.type': None})


class Pull(Command):
    metadata = {
        'name': 'pull',
        'type': 'dataset',
    }

    def execute(self):
        for model in self.obj.objects.values():
            assert model.source is None or isinstance(model.source, list)
            source = None
            for cmd in model.source:
                command, args = next(iter(cmd.items()))
                args.setdefault('source', source)
                source = self.run(model, {command: args})
            for row in source:
                data = {'type': model.name}
                for prop_name, prop in model.properties.items():
                    value = row
                    for cmd in prop.source:
                        command, args = next(iter(cmd.items()))
                        value = self.run(prop, {command: {**args, 'data': value}})
                    data[prop_name] = value
                yield data
