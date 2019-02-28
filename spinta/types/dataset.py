from spinta.commands import Command
from spinta.types import Type
from spinta.types.object import Object
from spinta.utils.refs import get_ref_id


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
            'backend': {'type': 'string', 'default': 'default'},
        },
    }


class Model(Object):
    metadata = {
        'name': 'dataset.model',
        'properties': {
            'source': {'type': 'command_list'},
            'identity': {'type': 'array', 'required': False},
            'properties': {'type': 'object', 'default': {}},
            'stars': {'type': 'integer'},
            'local': {'type': 'boolean'},
            'parent': {'type': 'dataset'},
            'backend': {'type': 'string', 'default': 'default', 'inherit': True},
        },
    }

    property_type = 'dataset.property'


class Property(Type):
    metadata = {
        'name': 'dataset.property',
        'properties': {
            'source': {'type': 'string'},
            'local': {'type': 'boolean'},
            'stars': {'type': 'integer'},
            'const': {'type': 'any'},
            'enum': {'type': 'array'},
            'parent': {'type': 'dataset.model'},
            'replace': {'type': 'object'},
            'ref': {'type': 'string'},
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


class CheckProperty(Command):
    metadata = {
        'name': 'manifest.check',
        'type': 'dataset',
    }

    def execute(self):
        for model in self.obj.objects.values():
            for prop in model.properties.values():
                if prop.ref and prop.ref not in self.obj.objects:
                    self.error(f"{model.name}.{prop.name} referenced an unknown object {prop.ref!r}.")


class Pull(Command):
    metadata = {
        'name': 'pull',
        'type': 'dataset',
    }

    def execute(self):
        for model in self.obj.objects.values():
            if model.source is None:
                continue
            for source in model.source:
                rows = self.run(model, source)
                for row in rows:
                    data = {'type': f'{model.name}/:source/{self.obj.name}'}
                    for prop in model.properties.values():
                        if isinstance(prop.source, list):
                            data[prop.name] = [
                                row[name]
                                for name in prop.source
                                if name in row
                            ]
                        else:
                            if prop.source in row:
                                data[prop.name] = row[prop.source]
                        if prop.ref and prop.name in data:
                            data[prop.name] = get_ref_id(data[prop.name])
                    if self.check_key(data.get('id')):
                        yield data

    def check_key(self, key):
        if isinstance(key, list):
            for k in key:
                if k is None:
                    return False
        elif key is None:
            return False
        return True
