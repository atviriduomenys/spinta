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

    def __init__(self, store, obj, args, backend, ns='default', stack: tuple = ()):
        self.store = store
        self.obj = obj
        self.backend = backend
        self.stack = stack
        self.ns = ns
        self.args = Args(args)
        self.ctx = {}

    def execute(self):
        raise NotImplementedError

    def inverse(self):
        # Undo everything, that was done with `execute`.
        raise NotImplementedError

    def run(self, *args, **kwargs):
        kwargs.setdefault('ns', self.ns)
        kwargs.setdefault('stack', self.stack + (self,))
        return self.store.run(*args, **kwargs)

    def load_object(self, *args, **kwargs):
        kwargs.setdefault('ns', self.ns)
        return self.store.load_object(*args, **kwargs)

    def error(self, message):
        for cmd in reversed(self.stack):
            if hasattr(cmd.obj, 'path'):
                path = cmd.obj.path
                raise Exception(f"{path}: {message}")
        raise Exception(message)


class Args:

    def __init__(self, args):
        self.args = args or {}

    def __getattr__(self, key):
        return self.args[key]

    def __call__(self, **kwargs):
        return {**self.args, **kwargs}
