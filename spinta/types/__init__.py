import copy
import pathlib


class _NA:

    def __repr__(self):
        return 'NA'


NA = _NA()


class MetaData:

    def __init__(self, cls):
        self.cls = cls
        self.metadata = cls.metadata
        self.name = self.metadata['name']
        self.bases = self._get_bases()
        self.properties = self._get_properties()

    def _get_bases(self):
        bases = []
        for cls in self.cls.mro():
            if hasattr(cls, 'metadata'):
                bases.append(cls)
            else:
                break
        return bases

    def _get_properties(self):
        properties = {}
        for cls in reversed(self.bases):
            if isinstance(cls.metadata, dict):
                properties.update(cls.metadata.get('properties', {}))
            else:
                properties.update(cls.metadata.metadata.get('properties', {}))
        return properties


class TypeMeta(type):

    def __new__(cls, name, bases, attrs):
        assert 'metadata' in attrs
        assert isinstance(attrs['metadata'], (dict, MetaData))
        assert isinstance(attrs['metadata'], dict) and 'name' in attrs['metadata']
        cls = super().__new__(cls, name, bases, attrs)
        cls.metadata = MetaData(cls)
        return cls


class Type(metaclass=TypeMeta):
    name = None
    metadata = {
        'name': None,
        'properties': {
            'type': {'type': 'string', 'required': True},
            'name': {'type': 'string', 'required': True},
            'title': {'type': 'string'},
            'description': {'default': ''},
        },
    }

    def __repr__(self):
        return f"<{self.name}: {self.metadata.name} <{self.__class__.__module__}.{self.__class__.__name__}>>"


class Function:
    # Function name in {'name': ...} expression.
    name = None

    # List of types for which this function is applicable.
    types = ()

    # Specify a backend name if this function applies only to a specific
    # backend.
    backend = None

    def __init__(self, manifest, obj: Type, backend, ns='default', stack: tuple = ()):
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
        for func in reversed(self.stack):
            if hasattr(func.obj, 'path'):
                path = func.obj.path
                raise Exception(f"{path}: {message}")
        raise Exception(message)


class ManifestLoad(Function):
    name = 'manifest.load'

    def execute(self, data: dict):
        assert isinstance(data, dict)

        for name, params in self.obj.metadata.properties.items():
            if name in data and data[name] is not NA:
                value = data[name]
            else:
                # Get default value.
                default = params.get('default', NA)
                if isinstance(default, (list, dict)):
                    value = copy.deepcopy(default)
                else:
                    value = default

                # Check if value is required.
                if params.get('required', False) and value is NA:
                    self.error(f"Parameter {name} is required.")

                # If value is not given, set it to None.
                if value is NA:
                    value = None

            # Set parameter on the spec object.
            setattr(self.obj, name, value)

        unknown_keys = set(data.keys()) - set(self.obj.metadata.properties.keys())
        if unknown_keys:
            keys = ', '.join(unknown_keys)
            self.error(f"{self.obj} does not have following parameters: {keys}.")


class Serialize(Function):
    name = 'serialize'

    def execute(self):
        output = {}
        for k, v in self.obj.metadata.properties.items():
            v = getattr(self.obj, k, v)
            if v is NA:
                continue
            if isinstance(v, Type):
                v = self.run(v, {'serialize': NA})
            if isinstance(v, pathlib.Path):
                v = str(v)
            output[k] = v
        return output
