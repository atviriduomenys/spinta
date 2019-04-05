import warnings

from spinta.components import Components


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

    def __init__(self, store, obj, name, args, *, value=None, base=0, backend=None, ns='default', stack: tuple = (), components: Components = None):
        self.store = store
        self.obj = obj
        self.name = name
        self.args = Args(args)
        self.value = value
        self.base = base
        self.backend = backend
        self.ns = ns
        self.stack = stack
        self.components = components or Components()

    def execute(self):
        base = self.base + 1
        if len(self.obj.metadata.bases) > base:
            return self.run(self.obj, {self.name: self.args.args}, base=base)
        raise NotImplementedError

    def run(self, *args, **kwargs):
        kwargs.setdefault('ns', self.ns)
        kwargs.setdefault('stack', self.stack + (self,))
        kwargs.setdefault('components', self.components)
        return self.store.run(*args, **kwargs)

    def load(self, *args, **kwargs):
        kwargs.setdefault('ns', self.ns)
        kwargs.setdefault('stack', self.stack + (self,))
        kwargs.setdefault('components', self.components)
        return self.store.load(*args, **kwargs)

    def error(self, message):
        stack = []
        for func in self.stack + (self,):
            params = [
                ('type', getattr(func.obj, 'type', None) or '(unknown)'),
                ('  name', getattr(func.obj, 'name', None)),
                ('  path', getattr(func.obj, 'path', None)),
                ('  class', f'{func.obj.__class__.__module__}.{func.obj.__class__.__name__}'),
                ('command', func.metadata.name or '(unknown)'),
                ('  class', f'{func.__class__.__module__}.{func.__class__.__name__}'),
                ('  backend', func.metadata.backend),
            ]
            stack.append('- ' + '\n  '.join(
                [f'{k}: {v}' for k, v in params if v]
            ) + '\n')
        stack = '\n'.join(filter(None, stack))
        raise CommandError(f"Command error:\n\n{stack}\n{message}")

    def deprecation(self, message):
        warnings.warn(message, DeprecationWarning, stacklevel=2)


class Args:

    def __init__(self, args):
        self.args = args or {}

    def __getattr__(self, key):
        return self.args[key]

    def __call__(self, **kwargs):
        return {**self.args, **kwargs}


class CommandError(Exception):
    pass
