import typing

from datetime import date, datetime

from spinta.commands import load, error, is_object_id
from spinta.components import Context, Manifest, Node, Model
from spinta.utils.schema import resolve_schema
from spinta.utils.errors import format_error
from spinta.common import NA
from spinta import commands

from spinta.exceptions import (
    ArrayTypeError,
    DateTypeError,
    DateTimeTypeError,
    IDInvalidError,
    ObjectTypeError,
    PropertyTypeError,
    UnknownObjectPropertiesError
)


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
        return value


class PrimaryKey(Type):
    pass


class Date(Type):

    def load(self, value: typing.Any):
        if value is None or value is NA:
            return value

        try:
            return date.fromisoformat(value)
        except (ValueError, TypeError):
            # FIXME: should inherit from PropertyError with `model` and `prop`
            # as context
            raise DateTypeError(date=value)


class DateTime(Type):

    def load(self, value: typing.Any):
        if value is None or value is NA:
            return value

        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            # FIXME: should inherit from PropertyError with `model` and `prop`
            # as context
            raise DateTimeTypeError(date=value)


class String(Type):
    schema = {
        'enum': {'type': 'array'},
    }

    def load(self, value: typing.Any):
        if value is None or value is NA:
            return value

        if isinstance(value, str):
            return value
        else:
            # FIXME: should inherit from PropertyError with `model` and `prop`
            # as context
            raise PropertyTypeError(self)


class Integer(Type):

    def load(self, value: typing.Any):
        if value is None or value is NA:
            return value

        if isinstance(value, int) and not isinstance(value, bool):
            return value
        else:
            # FIXME: should inherit from PropertyError with `model` and `prop`
            # as context
            raise PropertyTypeError(self)


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
        'object': {'type': 'string'},
        'enum': {'type': 'array'},
    }


class Array(Type):
    schema = {
        'items': {},
    }

    def load(self, value: typing.Any):
        if value is None or value is NA:
            return value

        if isinstance(value, list):
            return list(value)
        else:
            # FIXME: should inherit from PropertyError with `model` and `prop`
            # as context
            raise ArrayTypeError(value=value)


class Object(Type):
    schema = {
        'properties': {'type': 'object'},
    }

    def load(self, value: typing.Any):
        if value is None or value is NA:
            return {}

        if isinstance(value, dict):
            return dict(value)
        else:
            # FIXME: should inherit from PropertyError with `model` and `prop`
            # as context
            raise ObjectTypeError(value=value)


class File(Type):
    pass


@load.register()
def load(context: Context, type: Type, data: dict, manifest: Manifest) -> Type:
    return type


@load.register()
def load(context: Context, type: Object, data: dict, manifest: Manifest) -> Type:
    type.properties = {}
    for name, prop in data.get('properties', {}).items():
        place = type.prop.place + '.' + name
        prop = {
            'name': name,
            'place': place,
            'path': type.prop.path,
            'parent': type.prop,
            'model': type.prop.model,
            **prop,
        }
        type.prop.model.flatprops[place] = type.properties[name] = load(context, type.prop.__class__(), prop, type.prop.manifest)
    return type


@load.register()
def load(context: Context, type: Array, data: dict, manifest: Manifest) -> Type:
    if 'items' in data:
        prop = {
            'name': type.prop.name,
            'place': type.prop.place,
            'path': type.prop.path,
            'parent': type.prop,
            'model': type.prop.model,
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


@load.register()
def load(context: Context, type_: Type, value: object) -> object:
    # loads value to python native value according to given type
    return type_.load(value)


@load.register()
def load(context: Context, type_: PrimaryKey, value: object) -> list:
    model = type_.prop.model
    backend = model.backend
    if not is_object_id(context, backend, model, value):
        raise IDInvalidError()
    return value


@load.register()
def load(context: Context, type_: Array, value: object) -> list:
    # loads value into native python list, including all list items
    array_item_type = type_.items.type
    loaded_array = type_.load(value)
    if value is None or value is NA:
        return value
    new_loaded_array = []
    for item in loaded_array:
        new_loaded_array.append(load(context, array_item_type, item))
    return new_loaded_array


@load.register()
def load(context: Context, type_: Object, value: object) -> dict:
    # loads value into native python dict, including all dict's items
    loaded_obj = type_.load(value)

    # check that given obj does not have more keys, than type_'s schema
    unknown_params = set(loaded_obj.keys()) - set(type_.properties.keys())
    if unknown_params:
        raise UnknownObjectPropertiesError(
            model=type_.prop.model.name,
            prop=type_.prop.place,
            props_list=', '.join(map(repr, sorted(unknown_params))),
        )

    new_loaded_obj = {}
    for k, v in type_.properties.items():
        # only load value keys which are available in schema
        if k in loaded_obj:
            new_loaded_obj[k] = load(context, v.type, loaded_obj[k])
    return new_loaded_obj


@error.register()
def error(exc: Exception, context: Context, type_: Type):
    message = (
        '{exc}:\n'
        '  in type {type.name!r} {type}\n'
        '  in property {type.prop.name!r} {type.prop}>\n'
        '  in model {type.prop.parent.name!r} {type.prop.parent}>\n'
        '  in file {type.prop.path}\n'
    )
    raise Exception(format_error(message, {
        'exc': exc,
        'type': type_,
    }))


@error.register()
def error(exc: Exception, context: Context, type_: Type, value: object):
    message = (
        '{exc}:\n'
        '  in type {type.name!r} {type}\n'
        '  in property {type.prop.name!r} {type.prop}>\n'
        '  in model {type.prop.parent.name!r} {type.prop.parent}>\n'
        '  in file {type.prop.path}\n'
    )
    raise Exception(format_error(message, {
        'exc': exc,
        'type': type_,
    }))


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(model: Model, value: dict):
    return {
        prop.name: make_json_serializable(prop.type, value[prop.name])
        for prop in model.properties.values()
        if prop.name in value
    }

@commands.make_json_serializable.register()  # noqa
def make_json_serializable(type_: Type, value: object):
    return value


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(type_: DateTime, value: datetime):
    return value.isoformat()


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(type_: Date, value: date):
    if isinstance(value, datetime):
        return value.date().isoformat()
    else:
        return value.isoformat()


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(type_: Object, value: dict):
    return {
        prop.name: make_json_serializable(prop.type, value[prop.name])
        for prop in type_.properties.values()
        if prop.name in value
    }


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(type_: Array, value: type(None)):
    return {}


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(type_: Array, value: list):
    return [make_json_serializable(type_.items.type, v) for v in value]


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(type_: Array, value: type(None)):
    return []
