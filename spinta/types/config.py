from spinta.commands import Command
from spinta.types.object import Object


class Config(Object):
    metadata = {
        'name': 'config',
        'properties': {
            'backends': {'type': 'object', 'required': True},
            'manifests': {'type': 'object', 'required': True},
            'ignore': {'type': 'array', 'default': []},
        },
    }


class LoadConfig(Command):
    metadata = {
        'name': 'manifest.load',
        'type': 'config',
    }

    def execute(self):
        if self.ns != 'internal':
            self.error("Configuration can be loaded only on 'internal' namespace.")
        super().execute()


class LoadBackends(Command):
    metadata = {
        'name': 'manifest.load.backends',
        'type': 'config',
    }

    def execute(self):
        if 'default' not in self.obj.backends:
            self.error("'default' backend must be set in the configuration.")

        for name, backend in self.obj.backends.items():
            self.obj.backends[name] = self.load({'name': name, **backend})


class LoadManifests(Command):
    metadata = {
        'name': 'manifest.load.manifests',
        'type': 'config',
    }

    def execute(self):
        if 'default' not in self.obj.manifests:
            self.error("'default' manifest must be set in the configuration.")

        if 'internal' in self.obj.manifests:
            self.error("'internal' manifest can't be used in configuration, it is reserved for internal purposes.")

        for name, manifest in self.obj.manifests.items():
            self.store.objects[name] = {}
            self.obj.manifests[name] = self.load({'type': 'manifest', 'name': name, **manifest}, ns=name)
