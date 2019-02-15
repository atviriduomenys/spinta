from spinta.commands import Command
from spinta.types import Type
from spinta.types.object import Object


class Project(Type):
    metadata = {
        'name': 'project',
        'properties': {
            'path': {'type': 'path', 'required': True},
            'manifest': {'type': 'manifest', 'required': True},
            'version': {'type': 'integer', 'required': True},
            'date': {'type': 'date', 'required': True},
            'objects': {'type': 'object', 'default': {}},
            'impact': {'type': 'array', 'default': []},
            'url': {'type': 'url'},
            'source_code': {'type': 'url'},
            'owner': {'type': 'string'},
        },
    }


class Impact(Type):
    metadata = {
        'name': 'project.impact',
        'properties': {
            'year': {'type': 'integer', 'required': True},
            'users': {'type': 'integer'},
            'revenue': {'type': 'number'},
            'employees': {'type': 'integer'},
        },
    }


class Model(Object):
    metadata = {
        'name': 'project.model',
        'properties': {
            'properties': {'type': 'object', 'default': {}},
        },
    }

    property_type = 'project.property'


class Property(Type):
    metadata = {
        'name': 'project.property',
        'properties': {
            'enum': {'type': 'array'},
        },
    }


class LoadModel(Command):
    metadata = {
        'name': 'manifest.load',
        'type': 'project.model',
    }

    def execute(self):
        super().execute()
        assert isinstance(self.obj.properties, dict)
        for name, prop in self.obj.properties.items():
            self.obj.properties[name] = self.load({
                'type': 'project.property',
                'name': name,
                **(prop or {}),
            })


class CheckProject(Command):
    metadata = {
        'name': 'manifest.check',
        'type': 'project',
    }

    def execute(self):
        self.check_owner()
        self.check_impact()

    def check_owner(self):
        if self.obj.owner and self.obj.owner not in self.store.objects[self.ns]['owner']:
            self.error(f"Unknown owner {self.obj.owner}.")

    def check_impact(self):
        self.obj.impact = [
            self.load({'type': 'project.impact', 'name': str(i), **impact})
            for i, impact in enumerate(self.obj.impact)
        ]
