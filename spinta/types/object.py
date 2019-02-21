from spinta.types import Type
from spinta.commands import Command


class Object(Type):
    metadata = {
        'name': 'object',
        'properties': {
            'properties': {'type': 'object', 'default': {}},
        },
    }

    property_type = None


class LoadObject(Command):
    metadata = {
        'name': 'manifest.load',
        'type': 'object',
    }

    def execute(self):
        super().execute()
        assert isinstance(self.obj.properties, dict)
        for name, prop in self.obj.properties.items():
            data = {
                'name': name,
                'parent': self.obj,
                **(prop or {}),
            }
            if self.obj.property_type is not None:
                data['type'] = self.obj.property_type
            self.obj.properties[name] = self.load(data)


class PrepareObject(Command):
    metadata = {
        'name': 'prepare.type',
        'type': 'object',
    }

    def execute(self):
        super().execute()
        for prop in self.obj.properties.values():
            self.run(prop, {'prepare.type': None})


class CheckObject(Command):
    metadata = {
        'name': 'manifest.check',
        'type': 'object',
    }

    def execute(self):
        for prop in self.obj.properties.values():
            self.run(prop, {'manifest.check': None}, optional=True)
