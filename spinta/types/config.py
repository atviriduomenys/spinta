from spinta.commands import Command
from spinta.types.object import Object


class Config(Object):
    metadata = {
        'name': 'config',
        'properties': {
            'backends': {'type': 'object', 'required': True},
            'manifests': {'type': 'object', 'required': True},
        },
    }


class LoadConfig(Command):
    metadata = {
        'name': 'manifest.load',
        'type': 'config',
    }

    def execute(self):
        super().execute()

        if self.ns != 'internal':
            self.error("Configuration can be loaded only on 'internal' namespace.")

        if 'config' in self.store.objects[self.ns]:
            self.error("Config has been alread loaded.")
        self.store.objects[self.ns]['config'] = self.obj

        self.load_backends()
        self.load_manifests()

    def load_backends(self):
        if 'default' not in self.obj.backends:
            self.error("'default' backend must be set in the configuration.")

        for name, backend in self.obj.backends.items():
            self.obj.backends[name] = self.load({'name': name, **backend})

    def load_manifests(self):
        if 'default' not in self.obj.manifests:
            self.error("'default' manifest must be set in the configuration.")

        if 'internal' in self.obj.manifests:
            self.error("'internal' manifest can't be used in configuration, it is reserved for internal purposes.")

        for name, manifest in self.obj.manifests.items():
            self.store.objects[name] = {}
            self.obj.manifests[name] = self.load({'type': 'manifest', 'name': name, **manifest}, ns=name)
