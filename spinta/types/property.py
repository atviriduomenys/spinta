from spinta.types import Type
from spinta.commands import Command


class Property(Type):
    metadata = {
        'name': 'property',
        'properties': {
            'required': {'type': 'boolean', 'default': False},
            'unique': {'type': 'boolean', 'default': False},
            'const': {'type': 'any'},
            'default': {'type': 'any'},
            'check': {'type': 'command'},
        },
    }


class Any(Property):
    metadata = {
        'name': 'any',
    }


class PrimaryKey(Property):
    metadata = {
        'name': 'pk',
    }


class Date(Property):
    metadata = {
        'name': 'date',
    }


class String(Property):
    metadata = {
        'name': 'string',
        'properties': {
            'enum': {'type': 'array'},
        },
    }


class Integer(Property):
    metadata = {
        'name': 'integer',
    }


class Boolean(Property):
    metadata = {
        'name': 'boolean',
    }


class Ref(Property):
    metadata = {
        'name': 'ref',
        'properties': {
            'object': {'type': 'string'},
            'enum': {'type': 'array'},
        },
    }


class RefManifestCheck(Command):
    metadata = {
        'name': 'manifest.check',
        'type': 'ref',
    }

    def execute(self):
        if self.obj.object not in self.store.objects[self.ns]['model']:
            self.error(f"Unknown model {self.obj.object}.")


class BackRef(Property):
    metadata = {
        'name': 'backref',
        'properties': {
            'object': {'type': 'string'},
        },
    }


class Generic(Property):
    metadata = {
        'name': 'generic',
        'properties': {
            'model': {'type': 'string'},
            'enum': {'type': 'array'},
        },
    }
