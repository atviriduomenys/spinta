from spinta.types import Type, Function


class Property(Type):
    metadata = {
        'name': 'property',
        'properties': {
            'required': {'type': 'boolean', 'default': False},
            'unique': {'type': 'boolean', 'default': False},
            'const': {'type': 'any'},
            'default': {'type': 'any'},
            'check': {'type': 'function'},
        },
    }


class Any(Property):
    metadata = {
        'name': 'any',
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


class LinkTypesFunction(Function):
    name = 'link_types'
    types = ['backref']

    def execute(self):
        self.schema.object = self.manifest.objects['model'][self.schema.object]
        print('It worked!')
