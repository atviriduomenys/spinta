import datetime

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
            'null': {'type': 'boolean'},
            'check': {'type': 'command'},
            'link': {'type': 'string'},
            'parent': {'type': 'model'},
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


class PrepareDate(Command):
    metadata = {
        'name': 'prepare',
        'type': 'date',
    }

    def execute(self):
        value = super().execute()

        if isinstance(value, datetime.date):
            return value

        if value is not None:
            try:
                return datetime.datetime.strptime(value, '%Y-%m-%d').date()
            except ValueError as e:
                self.error(str(e))


class DateTime(Property):
    metadata = {
        'name': 'datetime',
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


class Number(Property):
    metadata = {
        'name': 'number',
    }


class Boolean(Property):
    metadata = {
        'name': 'boolean',
    }


class URL(Property):
    metadata = {
        'name': 'url',
    }


class Image(Property):
    metadata = {
        'name': 'image',
    }


class Spatial(Property):
    metadata = {
        'name': 'spatial',
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
            'property': {'type': 'string'},
            'secondary': {'type': 'string'},
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
