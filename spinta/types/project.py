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
            'parent': {'type': 'manifest'},
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
            'parent': {'type': 'project'},
        },
    }


class Model(Object):
    metadata = {
        'name': 'project.model',
        'properties': {
            'properties': {'type': 'object', 'default': {}},
            'parent': {'type': 'project'},
        },
    }

    property_type = 'project.property'


class Property(Type):
    metadata = {
        'name': 'project.property',
        'properties': {
            'enum': {'type': 'array'},
            'parent': {'type': 'project.model'},
        },
    }


class LoadProject(Command):
    metadata = {
        'name': 'manifest.load',
        'type': 'project',
    }

    def execute(self):
        super().execute()
        assert isinstance(self.obj.objects, dict)
        for name, obj in self.obj.objects.items():
            self.obj.objects[name] = self.load({
                'type': 'project.model',
                'name': name,
                'parent': self.obj,
                **(obj or {}),
            })


class PrepareProject(Command):
    metadata = {
        'name': 'prepare.type',
        'type': 'project',
    }

    def execute(self):
        super().execute()
        for model in self.obj.objects.values():
            self.run(model, {'prepare.type': None})


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
