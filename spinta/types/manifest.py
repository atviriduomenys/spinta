from ruamel.yaml import YAML
from ruamel.yaml.parser import ParserError

from spinta.commands import Command
from spinta.types import Type, NA
from spinta.types.type import ManifestLoad

yaml = YAML(typ='safe')


class Manifest(Type):
    metadata = {
        'name': 'manifest',
        'properties': {
            'path': {'type': 'path', 'required': True},
        },
    }


class ManifestLoadManifest(ManifestLoad):
    metadata = {
        'name': 'manifest.load',
        'type': 'manifest',
    }

    def execute(self, data):
        super().execute(data)

        for file in self.obj.path.glob('**/*.yml'):
            try:
                data = yaml.load(file.read_text())
            except ParserError as e:
                self.error(f"{file}: {e}.")
            if not isinstance(data, dict):
                self.error(f"{file}: expected dict got {data.__class__.__name__}.")
            data['path'] = file
            self.run(self.get_object(data), {'manifest.load': data})


class CheckManifest(Command):
    metadata = {
        'name': 'manifest.check',
        'type': 'manifest',
    }

    def execute(self):
        for objects in self.store.objects[self.ns].values():
            for obj in objects.values():
                self.run(obj, {'manifest.check': None}, optional=True)


class Serialize(Command):
    metadata = {
        'name': 'serialize',
        'type': 'manifest',
    }

    def execute(self):
        output = {}
        for object_type, objects in self.store.objects[self.ns].items():
            output[object_type] = {}
            for name, obj in objects.items():
                output[object_type][name] = self.run(obj, {'serialize': NA})
        return output


class PrepareBackend(Command):
    metadata = {
        'name': 'backend.prepare',
        'type': 'manifest',
    }

    def execute(self):
        for backend in self.store.config['backends'].keys():
            self.run(self.obj, {self.name: None}, backend=backend)
