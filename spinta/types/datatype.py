from __future__ import annotations

from typing import TYPE_CHECKING, Any, Union

import base64

from datetime import date, datetime

from spinta import spyna
from spinta import commands
from spinta import exceptions
from spinta.commands import load, is_object_id
from spinta.components import Context, Component, Property
from spinta.manifests.components import Manifest
from spinta.utils.schema import NA, NotAvailable
from spinta.hacks.spyna import binds_to_strs
from spinta.core.ufuncs import Expr
from spinta.types.helpers import check_no_extra_keys

if TYPE_CHECKING:
    from spinta.backends.components import Backend


class DataType(Component):
    schema = {
        'type': {'type': 'string', 'attr': 'name'},
        'unique': {'type': 'bool', 'default': False},
        'nullable': {'type': 'bool', 'default': False},
        'required': {'type': 'bool', 'default': False},
        'default': {'default': None},
        'prepare': {'type': 'spyna', 'default': None},
        'choices': {},
    }

    type: str
    unique: bool = False
    nullable: bool = False
    required: bool = False
    default: Any = None
    prepare: Expr = None
    choices: dict = None
    backend: Backend = None

    def __repr__(self):
        return f'<{self.prop.name}:{self.name}>'

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
        'model': {
            'type': 'string',
        },
        'refprops': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'enum': {'type': 'array'},
    }


class BackRef(DataType):
    schema = {
        'model': {'type': 'string'},
        'property': {'type': 'string'},
        'secondary': {'type': 'string'},
    }


class Generic(DataType):
    schema = {
        'model': {'type': 'string'},
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
        'backend': {'type': 'string', 'default': None},
        # TODO: add file hash, maybe 'sha1sum'
        # TODO: Maybe add all properties in schema as File.properties and maybe
        #       File should extend Object?
    }


class RQL(DataType):
    pass


class JSON(DataType):
    pass


@load.register(Context, DataType, dict, Manifest)
def load(context: Context, dtype: DataType, data: dict, manifest: Manifest) -> DataType:
    _set_backend(dtype)
    _add_leaf_props(dtype.prop)
    return dtype


def _set_backend(dtype: DataType):
    if dtype.backend:
        dtype.backend = dtype.prop.model.manifest.store.backends[dtype.backend]
    else:
        dtype.backend = dtype.prop.model.backend


@commands.link.register(Context, DataType)
def link(context: Context, dtype: DataType) -> None:
    pass


@load.register(Context, PrimaryKey, dict, Manifest)
def load(context: Context, dtype: PrimaryKey, data: dict, manifest: Manifest) -> DataType:
    dtype.unique = True
    if dtype.prop.name != '_id':
        raise exceptions.InvalidManagedPropertyName(dtype, name='_id')
    _set_backend(dtype)
    _add_leaf_props(dtype.prop)
    return dtype


def _add_leaf_props(prop: Property) -> None:
    if prop.name not in prop.model.leafprops:
        prop.model.leafprops[prop.name] = []
    prop.model.leafprops[prop.name].append(prop)


@load.register(Context, Object, dict, Manifest)
def load(context: Context, dtype: Object, data: dict, manifest: Manifest) -> DataType:
    _set_backend(dtype)
    props = {}
    for name, params in (dtype.properties or {}).items():
        place = dtype.prop.place + '.' + name
        prop = dtype.prop.__class__()
        prop.name = name
        prop.place = place
        prop.parent = dtype.prop
        prop.model = dtype.prop.model
        prop.list = dtype.prop.list
        load(context, prop, params, manifest)
        dtype.prop.model.flatprops[place] = prop
        props[name] = prop
    dtype.properties = props
    return dtype


@load.register(Context, Array, dict, Manifest)
def load(context: Context, dtype: Array, data: dict, manifest: Manifest) -> DataType:
    _set_backend(dtype)
    if dtype.items:
        assert isinstance(dtype.items, dict), type(dtype.items)
        prop = dtype.prop.__class__()
        prop.list = dtype.prop
        prop.name = dtype.prop.name
        prop.place = dtype.prop.place
        prop.parent = dtype.prop
        prop.model = dtype.prop.model
        load(context, prop, dtype.items, manifest)
        dtype.items = prop
    else:
        dtype.items = None
    return dtype


@load.register(Context, DataType, object)
def load(context: Context, dtype: DataType, value: object) -> object:
    # loads value to python native value according to given type
    return dtype.load(value)


@load.register(Context, PrimaryKey, object)
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


@load.register(Context, Array, object)
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


@load.register(Context, Object, object)
def load(context: Context, dtype: Object, value: object) -> dict:
    # loads value into native python dict, including all dict's items
    loaded_obj = dtype.load(value)

    non_hidden_keys = []
    for key, prop in dtype.properties.items():
        if not prop.hidden:
            non_hidden_keys.append(key)

    # check that given obj does not have more keys, than dtype's schema
    check_no_extra_keys(dtype, non_hidden_keys, loaded_obj)

    new_loaded_obj = {}
    for k, v in dtype.properties.items():
        # only load value keys which are available in schema
        if k in loaded_obj:
            new_loaded_obj[k] = load(context, v.dtype, loaded_obj[k])
    return new_loaded_obj


@load.register(Context, RQL, str)
def load(context: Context, dtype: RQL, value: str) -> dict:
    rql = spyna.parse(value)
    rql = binds_to_strs(rql)
    return rql


@commands.get_error_context.register(DataType)
def get_error_context(dtype: DataType, *, prefix='this'):
    context = commands.get_error_context(dtype.prop, prefix=f'{prefix}.prop')
    context['type'] = 'this.name'
    return context


@commands.rename_metadata.register(Context, dict)
def rename_metadata(context: Context, data: dict) -> dict:
    return data
