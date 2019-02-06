from spinta.types import Type, NA
from spinta.types.type import ManifestLoad, Serialize
from spinta.commands import Command


class Object(Type):
    metadata = {
        'name': 'object',
        'properties': {
            'properties': {'type': 'object', 'default': {}},
        },
    }


class ManifestLoadObject(ManifestLoad):
    name = 'manifest.load'
    types = ['object']

    def execute(self, data):
        super().execute(data)
        assert isinstance(self.obj.properties, dict)
        for name, prop in self.obj.properties.items():
            prop = {'name': name, **prop}
            obj = self.manifest.get_obj(prop)
            self.run(obj, {'manifest.load': prop})
            self.obj.properties[name] = obj


class ManifestCheck(Command):
    name = 'manifest.check'
    types = ['object']

    def execute(self):
        self.check_properties()
        self.check_primary_key()

    def check_properties(self):
        for prop in self.obj.properties.values():
            self.run(prop, {'manifest.check': None}, optional=True)

    def check_primary_key(self):
        primary_keys = []
        for prop in self.obj.properties.values():
            if prop.type == 'pk':
                primary_keys.append(prop)
        n_pkeys = len(primary_keys)
        if n_pkeys > 1:
            self.error(f"Only one primary key is allowed, found {n_pkeys}")
        elif n_pkeys == 0:
            self.error(f"At leas one primary key must be defined for model.")


class SerializeObject(Serialize):
    name = 'serialize'
    types = ['object']

    def execute(self):
        output = super().execute()
        for k, v in output['properties'].items():
            output['properties'][k] = self.run(v, {'serialize': NA})
        return output
