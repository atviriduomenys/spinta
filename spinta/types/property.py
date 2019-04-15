import datetime

from spinta.types import Type
from spinta.commands import load, check
from spinta.components import Context, Manifest
from spinta.backends import Backend


class Property(Type):
    metadata = {
        'name': 'property',
        'properties': {
            'required': {'type': 'boolean', 'default': False},
            'unique': {'type': 'boolean', 'default': False},
            'const': {'type': 'any'},
            'default': {'type': 'any'},
            'nullable': {'type': 'boolean'},
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


@load.register()
def load(context: Context, value: str, prop: Date, backend: Backend):
    try:
        return datetime.datetime.strptime(value, '%Y-%m-%d').date()
    except ValueError as e:
        context.error(str(e))


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


@check.register()
def check(context: Context, prop: Ref, manifest: Manifest):
    if prop.object not in manifest.objects['model']:
        context.error(f"Unknown model {prop.object}.")


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
