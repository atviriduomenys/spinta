import typing

from datetime import date, datetime

from spinta.commands import load, error
from spinta.components import Context, Manifest, Node
from spinta.exceptions import DataError
from spinta.utils.schema import resolve_schema
from spinta.utils.errors import format_error


class Type:

    schema = {
        'required': {'type': 'boolean', 'default': False},
        'unique': {'type': 'boolean', 'default': False},
        'const': {'type': 'any'},
        'default': {'type': 'any'},
        'nullable': {'type': 'boolean'},
        'check': {'type': 'command'},
        'link': {'type': 'string'},
    }

    def load(self, value: typing.Any):
        # loads value given by the user to native Python type
        # this is done before validation and writing to database
        raise NotImplementedError(f"{self.__class__} lacks implementation for load()")

    def is_valid(self, value: typing.Any):
        # checks if value is valid for the use (i.e. valid range, etc.)
        raise NotImplementedError(f"{self.__class__} lacks implementation for is_valid()")


class PrimaryKey(Type):

    def load(self, value: typing.Any):
        return str(value)

    def is_valid(self, value):
        # XXX: implement `pk` validation
        return True


class Date(Type):

    def load(self, value: typing.Any):
        try:
            return date.fromisoformat(value)
        except ValueError:
            raise DataError(f"Expected date value in ISO 8601 format ('YYYY-MM-DD'), got {value!r}")

    def is_valid(self, value: date):
        # self.load() ensures value is native `datetime` type
        return True


class DateTime(Type):

    def load(self, value: typing.Any):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            raise DataError(f"Expected datetime value in ISO 8601 format "
                            f"('YYYY-MM-DDTHH:MM:SS' or 'YYYY-MM-DDTHH:MM:SS.ffffff'), "
                            f"got {value!r}")

    def is_valid(self, value: datetime):
        # self.load() ensures value is native `datetime` type
        return True


class String(Type):
    schema = {
        'enum': {'type': 'array'},
    }

    def load(self, value: typing.Any):
        return str(value)

    def is_valid(self, value: str):
        # self.load() ensures value is native `str` type
        return True


class Integer(Type):

    def load(self, value: typing.Any):
        try:
            return int(value)
        except ValueError:
            raise DataError(f'Expected type int, got {type(value)}')

    def is_valid(self, value: int):
        # self.load() ensures value is native `int` type
        return True


class Number(Type):
    pass


class Boolean(Type):
    pass


class URL(Type):
    pass


class Image(Type):
    pass


class Spatial(Type):
    pass


class Ref(Type):
    schema = {
        'object': {'type': 'string'},
        'enum': {'type': 'array'},
    }

    def load(self, value: typing.Any):
        # TODO: implement `ref` loading
        return value

    def is_valid(self, value):
        # TODO: implement `ref` validation
        return True


class BackRef(Type):
    schema = {
        'object': {'type': 'string'},
        'property': {'type': 'string'},
        'secondary': {'type': 'string'},
    }


class Generic(Type):
    schema = {
        'model': {'type': 'string'},
        'enum': {'type': 'array'},
    }


class Array(Type):
    schema = {
        'items': {},
    }

    def load(self, value: typing.Any):
        if isinstance(value, list):
            # if value is list - return it
            return value
        else:
            raise DataError(f'Expected type array, got {type(value)}')

    def is_valid(self, value: list):
        # TODO: implement `array` validation
        return True

    def prepare_for_mongo(self, value: list) -> list:
        return value


class Object(Type):
    schema = {
        'properties': {'type': 'object'},
    }

    def is_valid(self, value):
        # TODO: implement `object` validation
        return True


class File(Type):
    pass


@load.register()
def load(context: Context, type: Type, data: dict, manifest: Manifest) -> Type:
    return type


@load.register()
def load(context: Context, type: Object, data: dict, manifest: Manifest) -> Type:
    type.properties = {}
    for name, prop in data.get('properties', {}).items():
        prop = {
            'name': name,
            'path': type.prop.path,
            'parent': type.prop,
            **prop,
        }
        type.properties[name] = load(context, type.prop.__class__(), prop, type.prop.manifest)
    return type


@load.register()
def load(context: Context, type: Array, data: dict, manifest: Manifest) -> Type:
    if 'items' in data:
        prop = {
            'name': type.prop.name,
            'path': type.prop.path,
            'parent': type.prop,
            **data['items'],
        }
        type.items = load(context, type.prop.__class__(), prop, type.prop.manifest)
    else:
        type.items = None
    return type


def load_type(context: Context, prop: Node, data: dict, manifest: Manifest):
    na = object()
    config = context.get('config')

    if prop.type not in config.components['types']:
        raise Exception(f"Unknown property type {prop.type!r}.")

    type = config.components['types'][prop.type]()
    type_schema = resolve_schema(type, Type)
    for name in type_schema:
        schema = type_schema[name]
        value = data.get(name, na)
        if schema.get('required', False) and value is na:
            raise Exception(f"Missing requied option {name!r}.")
        if value is na:
            value = schema.get('default')
        setattr(type, name, value)

    type.prop = prop
    type.name = data['type']

    return load(context, type, data, manifest)


@error.register()
def error(exc: Exception, context: Context, type: Type):
    message = (
        '{exc}:\n'
        '  in type {type.name!r} {type}\n'
        '  in property {type.prop.name!r} {type.prop}>\n'
        '  in model {type.prop.parent.name!r} {type.prop.parent}>\n'
        '  in file {type.prop.path}\n'
    )
    raise Exception(format_error(message, {
        'exc': exc,
        'type': type,
    }))
