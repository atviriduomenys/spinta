from spinta.commands import load
from spinta.components import Context, Node
from spinta.utils.schema import resolve_schema


class Type:

    schema = {
        'required': {'type': 'boolean', 'default': False},
        'unique': {'type': 'boolean', 'default': False},
        'const': {'type': 'any'},
        'default': {'type': 'any'},
        'nullable': {'type': 'boolean'},
        'check': {'type': 'command'},
        'link': {'type': 'string'},
        'parent': {'type': 'model'},
    }

    def __init__(self):
        self


class PrimaryKey(Type):
    pass


class Date(Type):
    pass


class DateTime(Type):
    pass


class String(Type):
    schema = {
        'enum': {'type': 'array'},
    }


class Integer(Type):
    pass


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


@load.register()
def load(context: Context, type: Type, data: dict, *, prop: Node) -> Type:
    return load_type(context, type, prop, data)


def load_type(context: Context, type: Type, prop: Node, data: dict):
    na = object()
    type_schema = resolve_schema(type, Type)
    type.prop = prop
    type.name = data['type']
    for name in type_schema:
        schema = type_schema[name]
        value = data.get(name, na)
        if schema.get('required', False) and value is na:
            _load_type_error(context, type, f"Missing requied option {name!r}.")
        if value is na:
            value = schema.get('default')
        setattr(type, name, value)
    return type


def _load_type_error(context, type, message):
    context.error(
        f"Error while loading <{type.name!r}: "
        f"{type.__class__.__module__}.{type.__class__.__name__}> on "
        f"<{type.prop.name!r}: {type.prop.__class__.__module__}."
        f"{type.prop.__class__.__name__}> from "
        f"{type.prop.path}: {message}"
    )
