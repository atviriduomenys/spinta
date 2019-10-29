import typing

from datetime import date, datetime

import pyrql

from spinta.commands import load, is_object_id
from spinta.components import Context, Manifest, Node
from spinta.utils.schema import resolve_schema
from spinta.common import NA
from spinta import commands
from spinta import exceptions


class DataType:

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


class PrimaryKey(DataType):
    pass


class Date(DataType):

    def load(self, value: typing.Any):
        if value is None or value is NA:
            return value

        try:
            return date.fromisoformat(value)
        except (ValueError, TypeError):
            raise exceptions.InvalidValue(self)


class DateTime(DataType):

    def load(self, value: typing.Any):
        if value is None or value is NA:
            return value

        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            raise exceptions.InvalidValue(self)


class String(DataType):
    schema = {
        'enum': {'type': 'array'},
    }

    def load(self, value: typing.Any):
        if value is None or value is NA:
            return value

        if isinstance(value, str):
            return value
        else:
            raise exceptions.InvalidValue(self)


class Integer(DataType):

    def load(self, value: typing.Any):
        if value is None or value is NA:
            return value

        if isinstance(value, int) and not isinstance(value, bool):
            return value
        else:
            raise exceptions.InvalidValue(self)


class Number(DataType):
    pass


class Boolean(DataType):
    pass


class URL(DataType):
    pass


class Image(DataType):
    pass


class Spatial(DataType):
    pass


class Ref(DataType):
    schema = {
        'object': {'type': 'string'},
        'enum': {'type': 'array'},
    }


class BackRef(DataType):
    schema = {
        'object': {'type': 'string'},
        'property': {'type': 'string'},
        'secondary': {'type': 'string'},
    }


class Generic(DataType):
    schema = {
        'object': {'type': 'string'},
        'enum': {'type': 'array'},
    }


class Array(DataType):
    schema = {
        'items': {},
    }

    def load(self, value: typing.Any):
        if value is None or value is NA:
            return value

        if isinstance(value, list):
            return list(value)
        else:
            raise exceptions.InvalidValue(self)


class Object(DataType):
    schema = {
        'properties': {'type': 'object'},
    }

    def load(self, value: typing.Any):
        if value is None or value is NA:
            return {}

        if isinstance(value, dict):
            return dict(value)
        else:
            raise exceptions.InvalidValue(self)


class File(DataType):
    pass


class RQL(DataType):
    pass


@load.register()
def load(context: Context, dtype: DataType, data: dict, manifest: Manifest) -> DataType:
    return dtype


@load.register()
def load(context: Context, dtype: Object, data: dict, manifest: Manifest) -> DataType:
    dtype.properties = {}
    for name, params in data.get('properties', {}).items():
        place = dtype.prop.place + '.' + name
        params = {
            'name': name,
            'place': place,
            'path': dtype.prop.path,
            'parent': dtype.prop,
            'model': dtype.prop.model,
            **params,
        }
        prop = load(
            context,
            dtype.prop.__class__(),
            params,
            dtype.prop.manifest,
        )
        dtype.prop.model.flatprops[place] = prop
        dtype.properties[name] = prop
    return dtype


@load.register()
def load(context: Context, dtype: Array, data: dict, manifest: Manifest) -> DataType:
    if 'items' in data:
        params = {
            'name': dtype.prop.name,
            'place': dtype.prop.place,
            'path': dtype.prop.path,
            'parent': dtype.prop,
            'model': dtype.prop.model,
            **data['items'],
        }
        dtype.items = load(context, dtype.prop.__class__(), params, dtype.prop.manifest)
    else:
        dtype.items = None
    return dtype


def load_type(context: Context, prop: Node, data: dict, manifest: Manifest):
    na = object()
    config = context.get('config')

    dtype = data.get('type')
    if dtype not in config.components['types']:
        raise Exception(f"Unknown property type {dtype!r}.")

    dtype = config.components['types'][dtype]()
    type_schema = resolve_schema(dtype, DataType)
    for name in type_schema:
        schema = type_schema[name]
        value = data.get(name, na)
        if schema.get('required', False) and value is na:
            raise Exception(f"Missing requied option {name!r}.")
        if value is na:
            value = schema.get('default')
        setattr(dtype, name, value)

    dtype.type = 'datatype'
    dtype.prop = prop
    dtype.name = data['type']

    return load(context, dtype, data, manifest)


@load.register()
def load(context: Context, dtype: DataType, value: object) -> object:
    # loads value to python native value according to given type
    return dtype.load(value)


@load.register()
def load(context: Context, dtype: PrimaryKey, value: object) -> list:
    if value is NA:
        return value
    if value is None:
        raise exceptions.InvalidValue(dtype)
    model = dtype.prop.model
    backend = model.backend
    if not is_object_id(context, backend, model, value):
        raise exceptions.InvalidValue(dtype)
    return value


@load.register()
def load(context: Context, dtype: Array, value: object) -> list:
    # loads value into native python list, including all list items
    array_item_type = dtype.items.dtype
    loaded_array = dtype.load(value)
    if value is None or value is NA:
        return value
    new_loaded_array = []
    for item in loaded_array:
        new_loaded_array.append(load(context, array_item_type, item))
    return new_loaded_array


@load.register()
def load(context: Context, dtype: Object, value: object) -> dict:
    # loads value into native python dict, including all dict's items
    loaded_obj = dtype.load(value)

    # check that given obj does not have more keys, than dtype's schema
    unknown_props = set(loaded_obj.keys()) - set(dtype.properties.keys())
    if unknown_props:
        raise exceptions.MultipleErrors(
            exceptions.FieldNotInResource(dtype.prop.model, property=f'{dtype.prop.place}.{prop}')
            for prop in sorted(unknown_props)
        )

    new_loaded_obj = {}
    for k, v in dtype.properties.items():
        # only load value keys which are available in schema
        if k in loaded_obj:
            new_loaded_obj[k] = load(context, v.dtype, loaded_obj[k])
    return new_loaded_obj


@load.register()
def load(context: Context, dtype: RQL, value: str) -> dict:
    rql = pyrql.parse(value)
    if rql['name'] == 'and':
        return rql['args']
    else:
        return [rql]


@commands.get_error_context.register()
def get_error_context(dtype: DataType, *, prefix='this'):
    context = commands.get_error_context(dtype.prop, prefix=f'{prefix}.prop')
    context['type'] = 'this.name'
    return context
