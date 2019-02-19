from ruamel.yaml import YAML
from ruamel.yaml.parser import ParserError
from ruamel.yaml.scanner import ScannerError

from spinta.utils.path import is_ignored
from spinta.commands import Command
from spinta.types import Type

yaml = YAML(typ='safe')


class Path(Type):
    metadata = {
        'name': 'path',
    }


class Manifest(Type):
    metadata = {
        'name': 'manifest',
        'properties': {
            'path': {'type': 'path', 'required': True},
            'parent': {'type': 'config'},
        },
    }


class ManifestLoadManifest(Command):
    metadata = {
        'name': 'manifest.load',
        'type': 'manifest',
    }

    def execute(self):
        super().execute()

        for file in self.obj.path.glob('**/*.yml'):
            if is_ignored(self.store.config.ignore, self.obj.path, file):
                continue

            try:
                data = yaml.load(file.read_text())
            except (ParserError, ScannerError) as e:
                self.error(f"{file}: {e}.")
            if not isinstance(data, dict):
                self.error(f"{file}: expected dict got {data.__class__.__name__}.")

            obj = self.load({
                'path': file,
                'parent': self.obj,
                **data,
            })

            if obj.type not in self.store.objects[self.ns]:
                self.store.objects[self.ns][obj.type] = {}

            if obj.name in self.store.objects[self.ns][obj.type]:
                self.error(f"Object {obj.type} with name {obj.name} already exist.")

            self.store.objects[self.ns][obj.type][obj.name] = obj


class PrepareManifest(Command):
    metadata = {
        'name': 'prepare.type',
        'type': 'manifest',
    }

    def execute(self):
        super().execute()
        for objects in self.store.objects[self.ns].values():
            for obj in objects.values():
                self.run(obj, {'prepare.type': None}, optional=True)


class CheckManifest(Command):
    metadata = {
        'name': 'manifest.check',
        'type': 'manifest',
    }

    def execute(self):
        for objects in self.store.objects[self.ns].values():
            for obj in objects.values():
                self.run(obj, {'manifest.check': None}, optional=True)


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
