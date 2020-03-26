from typing import Any, Iterable, Union

import base64

from datetime import date, datetime

from spinta import spyna
from spinta import commands
from spinta import exceptions
from spinta.commands import load, is_object_id
from spinta.components import Context, Manifest, Node, Property
from spinta.utils.schema import NA, NotAvailable, resolve_schema
from spinta.hacks.spyna import binds_to_strs


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

    def load(self, value: Any):
        return value


class PrimaryKey(DataType):
    pass


class Date(DataType):

    def load(self, value: Any):
        if value is None or value is NA or isinstance(value, (date, datetime)):
            return value

        if value == '':
            return None

        try:
            return date.fromisoformat(value)
        except (ValueError, TypeError):
            raise exceptions.InvalidValue(self)


class DateTime(DataType):

    def load(self, value: Any):
        if value is None or value is NA:
            return value

        if isinstance(value, datetime):
            return value

        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            raise exceptions.InvalidValue(self)


class String(DataType):
    schema = {
        'enum': {'type': 'array'},
    }

    def load(self, value: Any):
        if value is None or value is NA:
            return value

        if isinstance(value, str):
            return value
        else:
            raise exceptions.InvalidValue(self)


class Binary(DataType):
    schema = {}

    def load(
        self,
        value: Union[bytes, str, NotAvailable, None],
    ) -> Union[bytes, NotAvailable, None]:
        if value is None or value is NA:
            return value
        if isinstance(value, str):
            return base64.b64decode(value)
        elif isinstance(value, bytes):
            return value
        else:
            raise exceptions.InvalidValue(self)


class Integer(DataType):

    def load(self, value: Any):
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

    def load(self, value: Any):
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

    def load(self, value: Any):
        if value is None or value is NA:
            return {}

        if isinstance(value, dict):
            return dict(value)
        else:
            raise exceptions.InvalidValue(self)


class File(DataType):
    schema = {
        '_id': {'type': 'string'},
        '_content_type': {'type': 'string'},
        '_content': {'type': 'binary'},
        # TODO: add file hash, maybe 'sha1sum'
        # TODO: Maybe add all properties in schema as File.properties and maybe
        #       File should extend Object?
    }


class RQL(DataType):
    pass


class JSON(DataType):
    pass


@load.register()
def load(context: Context, dtype: DataType, data: dict, manifest: Manifest) -> DataType:
    _add_leaf_props(dtype.prop)
    return dtype


@load.register()
def load(context: Context, dtype: PrimaryKey) -> None:
    if dtype.prop.name != '_id':
        raise exceptions.InvalidManagedPropertyName(dtype, name='_id')
    _add_leaf_props(dtype.prop)


@commands.init()
def init(context: Context, dtype: PrimaryKey) -> None:
    dtype.unique = True


def _add_leaf_props(prop: Property) -> None:
    if prop.name not in prop.model.leafprops:
        prop.model.leafprops[prop.name] = []
    prop.model.leafprops[prop.name].append(prop)


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
        prop = dtype.prop.__class__()
        prop.list = dtype.prop.list
        prop = load(
            context,
            prop,
            params,
            dtype.prop.manifest,
        )
        dtype.prop.model.flatprops[place] = prop
        dtype.properties[name] = prop
    return dtype


@load.register()
def load(context: Context, dtype: Array, data: dict, manifest: Manifest) -> DataType:
    if 'items' in data:
        assert isinstance(data['items'], dict), type(data['items'])
        params = {
            'name': dtype.prop.name,
            'place': dtype.prop.place,
            'path': dtype.prop.path,
            'parent': dtype.prop,
            'model': dtype.prop.model,
            **data['items'],
        }
        prop = dtype.prop.__class__()
        prop.list = dtype.prop
        prop = load(context, prop, params, dtype.prop.manifest)
        dtype.items = prop
    else:
        dtype.items = None
    return dtype


@load.register()
def load(context: Context, dtype: DataType, value: object) -> object:
    # loads value to python native value according to given type
    return dtype.load(value)


@load.register()
def load(context: Context, dtype: File, value: object) -> object:
    # loads value into native python dict, including all dict's items
    loaded_obj = dtype.load(value)
    if value is NA:
        return value
    # check that given obj does not have more keys, than dtype's schema
    _check_no_extra_keys(dtype, dtype.schema, loaded_obj)

    if '_content' in value and isinstance(value['_content'], str):
        value['_content'] = base64.b64decode(value['_content'])

    return loaded_obj


@load.register()
def load(context: Context, dtype: PrimaryKey, value: object) -> object:
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

    non_hidden_keys = []
    for key, prop in dtype.properties.items():
        if not prop.hidden:
            non_hidden_keys.append(key)

    # check that given obj does not have more keys, than dtype's schema
    _check_no_extra_keys(dtype, non_hidden_keys, loaded_obj)

    new_loaded_obj = {}
    for k, v in dtype.properties.items():
        # only load value keys which are available in schema
        if k in loaded_obj:
            new_loaded_obj[k] = load(context, v.dtype, loaded_obj[k])
    return new_loaded_obj


def _check_no_extra_keys(dtype: DataType, schema: Iterable, data: Iterable):
    unknown = set(data) - set(schema)
    if unknown:
        raise exceptions.MultipleErrors(
            exceptions.FieldNotInResource(
                dtype.prop,
                property=f'{dtype.prop.place}.{prop}',
            )
            for prop in sorted(unknown)
        )


@load.register()
def load(context: Context, dtype: RQL, value: str) -> dict:
    rql = spyna.parse(value)
    rql = binds_to_strs(rql)
    if rql['name'] == 'and':
        return rql['args']
    else:
        return [rql]


@commands.get_error_context.register()
def get_error_context(dtype: DataType, *, prefix='this'):
    context = commands.get_error_context(dtype.prop, prefix=f'{prefix}.prop')
    context['type'] = 'this.name'
    return context


@commands.rename_metadata.register()
def rename_metadata(context: Context, data: dict) -> dict:
    return data
