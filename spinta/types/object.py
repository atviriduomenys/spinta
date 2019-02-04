from spinta.types import Function, NA
from spinta.types import Type, LoadType
from spinta.types import Serialize


class Object(Type):
    metadata = {
        'name': 'object',
        'properties': {
            'properties': {'type': 'object', 'default': {}},
        },
    }


class LoadObject(LoadType):
    name = 'load'
    types = ('object',)

    def execute(self, data):
        super().execute(data)
        assert isinstance(self.schema.properties, dict)
        for name, prop in self.schema.properties.items():
            prop = {'name': name, **prop}
            type = self.manifest.get_type(prop)
            self.run(type, {'load': prop})
            self.schema.properties[name] = type


class LinkTypesFunction(Function):
    name = 'link'
    types = ['object']

    def execute(self):
        for prop in self.schema.properties.values():
            self.run(prop, {'link': NA}, optional=True)


class SerializeObject(Serialize):
    name = 'serialize'
    types = ['object']

    def execute(self):
        output = super().execute()
        for k, v in output['properties'].items():
            output['properties'][k] = self.run(v, {'serialize': NA})
        return output
