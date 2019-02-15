from spinta.commands import Command
from spinta.types import Type
from spinta.types.object import Object


class Dataset(Type):
    metadata = {
        'name': 'dataset',
        'properties': {
            'path': {'type': 'path', 'required': True},
            'manifest': {'type': 'manifest', 'required': True},
            'source': {'type': ['object', 'string', 'array']},
            'objects': {'type': 'object', 'default': {}},
            'version': {'type': 'integer', 'required': True},
            'date': {'type': 'date', 'required': True},
            'owner': {'type': 'string'},
            'stars': {'type': 'integer'},
        },
    }


class Model(Object):
    metadata = {
        'name': 'dataset.model',
        'properties': {
            'source': {'type': 'command'},
            'identity': {'type': ['string', 'object']},
            'properties': {'type': 'object', 'default': {}},
            'stars': {'type': 'integer'},
            'local': {'type': 'boolean'},
        },
    }

    property_type = 'dataset.property'


class Property(Type):
    metadata = {
        'name': 'dataset.property',
        'properties': {
            'source': {'type': ['string', 'object']},
            'local': {'type': 'boolean'},
            'stars': {'type': 'integer'},
            'const': {'type': 'any'},
            'enum': {'type': 'array'},
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
                **(obj or {}),
            })


class Pull(Command):
    metadata = {
        'name': 'pull',
        'type': 'dataset',
    }

    def execute(self):
        for model in self.obj.objects.values():
            source = self.run(model, model.source)
            for row in source:
                data = {'type': model.name}
                for prop_name, prop in model.properties.items():
                    if isinstance(prop.source, str):
                        command, _ = next(iter(model.source.items()))
                        args = {'name': prop.source}
                    else:
                        command, args = next(iter(prop.source.items()))
                    data[prop_name] = self.run(prop, {command: {**args, 'source': row}})
                yield data
