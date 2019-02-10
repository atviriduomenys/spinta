from spinta.types import Type
from spinta.types.type import ManifestLoad
from spinta.commands import Command


class Object(Type):
    metadata = {
        'name': 'object',
        'properties': {
            'properties': {'type': 'object', 'default': {}},
        },
    }


class ManifestLoadObject(ManifestLoad):
    metadata = {
        'name': 'manifest.load',
        'type': 'object',
    }

    def execute(self):
        super().execute()
        assert isinstance(self.obj.properties, dict)
        for name, prop in self.obj.properties.items():
            self.obj.properties[name] = self.load_object({'name': name, **prop})


class ManifestCheck(Command):
    metadata = {
        'name': 'manifest.check',
        'type': 'object',
    }

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


class Project(Object):
    metadata = {
        'name': 'project',
    }


class Owner(Object):
    metadata = {
        'name': 'owner',
    }
