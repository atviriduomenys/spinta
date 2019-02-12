from spinta.types import Type
from spinta.commands import Command


class Object(Type):
    metadata = {
        'name': 'object',
        'properties': {
            'properties': {'type': 'object', 'default': {}},
        },
    }


class LoadObject(Command):
    metadata = {
        'name': 'manifest.load',
        'type': 'object',
    }

    def execute(self):
        super().execute()
        assert isinstance(self.obj.properties, dict)
        for name, prop in self.obj.properties.items():
            self.obj.properties[name] = self.load({'name': name, **prop})


class CheckObject(Command):
    metadata = {
        'name': 'manifest.check',
        'type': 'object',
    }

    def execute(self):
        for prop in self.obj.properties.values():
            self.run(prop, {'manifest.check': None}, optional=True)


class Project(Object):
    metadata = {
        'name': 'project',
    }


class Owner(Object):
    metadata = {
        'name': 'owner',
    }
