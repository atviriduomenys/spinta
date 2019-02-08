from ruamel.yaml import YAML
from ruamel.yaml.parser import ParserError

from spinta.commands import Command
from spinta.types import Type
from spinta.types.type import ManifestLoad, Serialize

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

    def execute(self):
        super().execute()

        for file in self.obj.path.glob('**/*.yml'):
            try:
                data = yaml.load(file.read_text())
            except ParserError as e:
                self.error(f"{file}: {e}.")
            if not isinstance(data, dict):
                self.error(f"{file}: expected dict got {data.__class__.__name__}.")
            self.load_object({'path': file, **data})


class CheckManifest(Command):
    metadata = {
        'name': 'manifest.check',
        'type': 'manifest',
    }

    def execute(self):
        for objects in self.store.objects[self.ns].values():
            for obj in objects.values():
                self.run(obj, {'manifest.check': None}, optional=True)


class SerializeManifest(Serialize, Command):
    metadata = {
        'name': 'serialize',
        'type': 'manifest',
    }

    def execute(self):
        output = {}
        for object_type, objects in self.store.objects[self.ns].items():
            output[object_type] = {}
            for name, obj in objects.items():
                output[object_type][name] = self.run(obj, {'serialize': self.args(level=self.args.level + 1)})
        return output


class BackendPrepare(Command):
    metadata = {
        'name': 'backend.prepare',
        'type': 'manifest',
    }

    def execute(self):
        for backend in self.store.config.backends.keys():
            self.run(self.obj, {'backend.prepare': None}, backend=backend)


class BackendMigrate(Command):
    metadata = {
        'name': 'backend.migrate',
        'type': 'manifest',
    }

    def execute(self):
        for backend in self.store.config.backends.keys():
            self.run(self.obj, {'backend.migrate': None}, backend=backend)
