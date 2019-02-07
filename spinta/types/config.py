from spinta.types.object import Object, ManifestLoadObject


class Config(Object):
    metadata = {
        'name': 'config',
        'properties': {
            'backends': {'type': 'object', 'required': True},
            'manifests': {'type': 'object', 'required': True},
        },
    }


class ManifestLoadConfig(ManifestLoadObject):
    metadata = {
        'name': 'manifest.load',
        'type': 'config',
    }

    def execute(self, data):
        super().execute(data)

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
            obj = self.get_object(backend)
            self.run(obj, {'manifest.load': {'name': name, **backend}})
            self.obj.backends[name] = obj

    def load_manifests(self):
        if 'default' not in self.obj.manifests:
            self.error("'default' manifest must be set in the configuration.")

        if 'internal' in self.obj.manifests:
            self.error("'internal' manifest can't be used in configuration, it is reserved for internal purposes.")

        for name, manifest in self.obj.manifests.items():
            manifest = {'type': 'manifest', 'name': name, **manifest}
            self.store.objects[name] = {}
            obj = self.get_object(manifest)
            self.run(obj, {'manifest.load': manifest}, ns=name)
            self.obj.manifests[name] = obj
