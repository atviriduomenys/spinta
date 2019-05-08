from datetime import datetime

from spinta.commands import load, error
from spinta.components import Context, Node
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

    def load(self, value):
        # loads value to native Python type
        raise NotImplementedError(f"{self.__class__} lacks implementation for load()")

    def is_valid(self, value):
        # checks if value is valid for the use (i.e. valid range, etc.)
        raise NotImplementedError(f"{self.__class__} lacks implementation for is_valid()")


class PrimaryKey(Type):

    def load(self, value):
        return str(value)

    def is_valid(self, value):
        # XXX: implement `pk` validation
        return True


class Date(Type):

    DEFAULT_FMT = "%Y-%m-%d"

    def load(self, value, date_fmt=DEFAULT_FMT):
        return datetime.strptime(value, date_fmt)

    def is_valid(self, value, date_fmt=DEFAULT_FMT):
        # self.load() ensures value is native `datetime` type
        return True



class DateTime(Type):

    DEFAULT_FMT = "%Y-%m-%d %H:%M:%S"

    def load(self, value, date_fmt=DEFAULT_FMT):
        return datetime.strptime(value, date_fmt)

    def is_valid(self, value, date_fmt=DEFAULT_FMT):
        # self.load() ensures value is native `datetime` type
        return True


class String(Type):
    schema = {
        'enum': {'type': 'array'},
    }

    def load(self, value):
        return str(value)

    def is_valid(self, value):
        # self.load() ensures value is native `str` type
        return True


class Integer(Type):

    def load(self, value):
        return int(value)

    def is_valid(self, value):
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

    def load(self, value):
        # XXX: implement `ref` loading
        return value

    def is_valid(self, value):
        # XXX: implement `ref` validation
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

    def load(self, value):
        if isinstance(value, list):
            # if value is list - return it
            return value
        else:
            # if value isn't a list - add it to list and return it
            return [value]

    def is_valid(self, value):
        # XXX: implement `array` validation
        return True


class Object(Type):
    schema = {
        'properties': {'type': 'object'},
    }

    def is_valid(self, value):
        # XXX: implement `object` validation
        return True


class File(Type):
    pass


@load.register()
def load(context: Context, type: Type, data: dict) -> Type:
    return type


def load_type(context: Context, prop: Node, data: dict):
    na = object()
    config = context.get('config')

    if prop.type not in config.components['types']:
        _load_type_error(context, prop, prop.type, f"Unknown property type {prop.type!r}.")

    type = config.components['types'][prop.type]()
    type_schema = resolve_schema(type, Type)
    for name in type_schema:
        schema = type_schema[name]
        value = data.get(name, na)
        if schema.get('required', False) and value is na:
            _load_type_error(context, prop, type, f"Missing requied option {name!r}.")
        if value is na:
            value = schema.get('default')
        setattr(type, name, value)

    type.prop = prop
    type.name = data['type']

    return load(context, type, data)


def _load_type_error(context: Context, prop: Node, type: Type, message: str):
    if isinstance(type, Type):
        type_repr = f"<{type.name!r}: {type.__class__.__module__}.{type.__class__.__name__}>"
    else:
        type_repr = f"<{type!r}: None>"
    context.error(
        f"Error while loading {type_repr} on "
        f"<{prop.name!r}: {prop.__class__.__module__}."
        f"{prop.__class__.__name__}> from "
        f"{prop.path}: {message}"
    )


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
