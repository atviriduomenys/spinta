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


class Type:
    name = None
    properties = {
        'type': {'type': 'string', 'required': True},
        'name': {'type': 'string', 'unique': True, 'required': True},
        'title': {'type': 'string'},
        'description': {'default': ''},
    }

    def __init__(self, data):
        self.properties = {}
        for cls in reversed(self.parents()):
            self.properties.update(cls.properties)

        self.data = {}
        for k, v in self.properties.items():
            self.data[k] = data.get(k, v.get('default', NA))

    def __setitem__(self, name, value):
        if name not in self.properties:
            raise Exception(f"Unknown property {name!r} of {self.name!r} object.")
        self.data[name] == value

    def __getitem__(self, name):
        return self.data[name]

    def parents(self):
        parents = []
        for cls in self.__class__.mro():
            parents.append(cls)
            if cls is Type:
                break
        return parents


class LoadObject(Function):
    name = 'load'

    def execute(self, schema):
        assert isinstance(schema, dict)
        assert 'type' in schema
        assert schema['type'] in self.manifest.types
        Class = self.manifest.types[schema['type']]
        obj = Class(schema)
        return obj
