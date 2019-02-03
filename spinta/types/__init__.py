class Function:
    # List of types for which this function is applicable.
    types = ()

    # Function name in {'name': ...} expression.
    name = None

    # Available function arguments.
    arguments = {}

    def __init__(self, manifest, obj):
        self.manifest = manifest
        self.obj = obj

    def prepare(self, value):
        return value

    def execute(self, *args, **kwargs):
        raise NotImplementedError

    def error(self, message):
        self.ctx.error(message)


class _NA:

    def __repr__(self):
        return 'NA'


NA = _NA()

class MetaData:

    def __init__(self, name):
        self.cls = cls
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
            properties.update(self.metadata.get('properties', {}))
        return properties


class Type():
    metadata = MetaData(
        name=None,
        properties={
            'type': {'type': 'string', 'required': True},
            'name': {'type': 'string', 'unique': True, 'required': True},
            'title': {'type': 'string'},
            'description': {'default': ''},
        },
    )

    def __init__(self, data):
        assert isinstance(data, dict)
        for name, params in self.metadata.properties.items():
            setattr(self, name, data.get(name, params.get('default', NA)))

    def __repr__(self):
        return f"<{self.__class__.__module__}.{self.__class__.__name__}: {self.type}: {self.name}>"


class LoadType(Function):
    name = 'load'

    def execute(self):
        pass
