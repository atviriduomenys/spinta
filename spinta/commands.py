class MetaData:

    def __init__(self, cls):
        self.name = cls.metadata['name']
        self.type = cls.metadata.get('type', None)
        if not isinstance(self.type, tuple):
            self.type = (self.type,)
        self.backend = cls.metadata.get('backend', None)


class MetaClass(type):

    def __new__(cls, name, bases, attrs):
        assert 'metadata' in attrs
        assert isinstance(attrs['metadata'], (dict, MetaData))
        assert isinstance(attrs['metadata'], dict) and 'name' in attrs['metadata']
        cls = super().__new__(cls, name, bases, attrs)
        cls.metadata = MetaData(cls)
        return cls


class Command(metaclass=MetaClass):
    metadata = {'name': None, 'type': None, 'backend': None}

    def __init__(self, manifest, obj, backend, ns='default', stack: tuple = ()):
        self.manifest = manifest
        self.obj = obj
        self.backend = backend
        self.stack = stack
        self.ns = ns

    def execute(self, *args, **kwargs):
        raise NotImplementedError

    def run(self, *args, **kwargs):
        kwargs.setdefault('ns', self.ns)
        kwargs.setdefault('stack', self.stack + (self,))
        return self.manifest.run(*args, **kwargs)

    def error(self, message):
        for cmd in reversed(self.stack):
            if hasattr(cmd.obj, 'path'):
                path = cmd.obj.path
                raise Exception(f"{path}: {message}")
        raise Exception(message)


AVAILABLE_COMMANDS = {
    'backend.migrate',
    'backend.migrate.internal',
    'backend.prepare',
    'backend.prepare.internal',
    'manifest.check',
    'manifest.load',
    'serialize',
}
