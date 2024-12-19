from __future__ import annotations

import base64
import uuid
from datetime import date, time, datetime
from typing import Dict
from typing import List
from typing import TYPE_CHECKING, Any, Union

from spinta import commands
from spinta import exceptions
from spinta import spyna
from spinta.commands import load, is_object_id
from spinta.components import Context, Component, Property
from spinta.components import Model
from spinta.core.ufuncs import Expr
from spinta.manifests.components import Manifest
from spinta.manifests.tabular.constants import DataTypeEnum
from spinta.types.helpers import check_no_extra_keys
from spinta.types.helpers import set_dtype_backend
from spinta.utils.schema import NA, NotAvailable
from spinta.utils.types import is_str_uuid

if TYPE_CHECKING:
    from spinta.backends.components import Backend


class DataType(Component):
    schema = {
        'type': {'type': 'string', 'attr': 'name'},
        'type_args': {'type': 'array'},
        'unique': {'type': 'bool', 'default': False},
        'nullable': {'type': 'bool', 'default': False},
        'required': {'type': 'bool', 'default': False},
        'default': {'default': None},
        'prepare': {'type': 'spyna', 'default': None},
        'choices': {}
    }

    type: str
    type_args: List[str]
    name: str
    unique: bool = False
    nullable: bool = False
    required: bool = False
    default: Any = None
    prepare: Expr = None
    choices: dict = None
    backend: Backend = None
    prop: Property = None
    expandable: bool = False
    requires_source: bool = True
    inherited: bool = False

    def __repr__(self):
        return f'<{self.prop.name}:{self.name}>'

    def load(self, value: Any):
        return value

    def get_bind_expr(self):
        return Expr('bind', self.prop.name)

    def __copy__(self):
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result


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


class Time(DataType):

    def load(self, value: Any):
        if value is None or value is NA or isinstance(value, time):
            return value

        if value == '':
            return None

        try:
            return time.fromisoformat(value)
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
            raise exceptions.InvalidValue(self, value=value)


class Number(DataType):
    pass


class Boolean(DataType):
    pass


class URL(String):
    pass


class URI(String):
    pass


class Ref(DataType):
    # Referenced model
    model: Model
    # Properties from referenced model
    refprops: List[Property]
    # True if ref column is set explicitly
    explicit: bool = False
    # Denorm properties
    properties: Dict[str, Property] = {}

    schema = {
        'model': {
            'type': 'string',
        },
        'refprops': {
            'type': 'array',
            'items': {'type': 'string'},
        },
        'enum': {'type': 'array'},
        'properties': {'type': 'object'},
    }


class BackRef(DataType):
    model: Model
    refprop: Property
    explicit: bool = False
    properties: Dict[str, Property] = {}

    schema = {
        'model': {'type': 'string'},
        'refprop': {'type': 'string'},
        'properties': {'type': 'object'},
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

    items: Property = None
    expandable = True

    def load(self, value: Any):
        if value is None or value is NA:
            return value

        if isinstance(value, list):
            return list(value)
        else:
            raise exceptions.InvalidValue(self)


class ArrayBackRef(DataType):
    schema = {
        'items': {},
    }

    items: Property = None
    expandable = True

    def load(self, value: Any):
        if value is None or value is NA:
            return value

        if isinstance(value, list):
            return list(value)
        else:
            raise exceptions.InvalidValue(self)

    def get_type_repr(self):
        return "array"


class Partial(DataType):
    schema = {
        'properties': {'type': 'object'},
    }
    properties: Dict[str, Property] = None


class Object(DataType):
    schema = {
        'properties': {'type': 'object'},
    }

    properties: Dict[str, Property] = None
    requires_source = False

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


class Image(File):
    pass


class RQL(DataType):
    pass


class JSON(DataType):
    pass


class Denorm(DataType):
    rel_prop: Property
    inherited = True
    requires_source = False

    def get_type_repr(self):
        return ""


class ExternalRef(Ref):
    def get_type_repr(self):
        return "ref"


class Inherit(DataType):
    inherited = True

    def get_type_repr(self):
        return ""


class PartialArray(Array):
    pass


class PageType(DataType):
    pass


class UUID(DataType):
    def load(self, value: Any):
        if value is None or value is NA:
            return value

        if isinstance(value, uuid.UUID):
            return value

        if isinstance(value, str):
            if is_str_uuid(value):
                return uuid.UUID(value)
            else:
                raise exceptions.InvalidValue(self, value=value)

        raise exceptions.InvalidValue(self, value=value)


@commands.check.register(Context, DataType)
def check(context: Context, dtype: DataType):
    pass


@load.register(Context, DataType, dict, Manifest)
def load(context: Context, dtype: DataType, data: dict, manifest: Manifest) -> DataType:
    _add_leaf_props(dtype.prop)
    return dtype


@commands.link.register(Context, DataType)
def link(context: Context, dtype: DataType) -> None:
    set_dtype_backend(dtype)


@load.register(Context, UUID, dict, Manifest)
def load(context: Context, dtype: UUID, data: dict, manifest: Manifest) -> UUID:
    _load = commands.load[Context, DataType, dict, Manifest]
    dtype: UUID = _load(context, dtype, data, manifest)
    return dtype


@load.register(Context, URI, dict, Manifest)
def load(context: Context, dtype: URI, data: dict, manifest: Manifest) -> URI:
    _load = commands.load[Context, DataType, dict, Manifest]
    dtype: URI = _load(context, dtype, data, manifest)

    prop = dtype.prop
    model = prop.model
    if model.uri is not None and prop.uri == model.uri:
        if model.uri_prop is None:
            model.uri_prop = prop
            dtype.unique = True
        else:
            raise exceptions.TooManyModelUriProperties(dtype, uri_prop=model.uri_prop.name)

    return dtype


@load.register(Context, PrimaryKey, dict, Manifest)
def load(context: Context, dtype: PrimaryKey, data: dict, manifest: Manifest) -> DataType:
    dtype.unique = True
    if dtype.prop.name != '_id':
        raise exceptions.InvalidManagedPropertyName(dtype, name='_id')
    _add_leaf_props(dtype.prop)
    return dtype


def _add_leaf_props(prop: Property) -> None:
    if prop.name not in prop.model.leafprops:
        prop.model.leafprops[prop.name] = []
    prop.model.leafprops[prop.name].append(prop)


@load.register(Context, Partial, dict, Manifest)
def load(context: Context, dtype: Partial, data: dict, manifest: Manifest) -> DataType:
    return _load_properties(context, dtype, manifest)


@load.register(Context, Ref, dict, Manifest)
def load(context: Context, dtype: Ref, data: dict, manifest: Manifest) -> DataType:
    dtype = _load_properties(context, dtype, manifest)
    _add_leaf_props(dtype.prop)
    return dtype


@load.register(Context, BackRef, dict, Manifest)
def load(context: Context, dtype: BackRef, data: dict, manifest: Manifest) -> DataType:
    dtype = _load_properties(context, dtype, manifest)
    _add_leaf_props(dtype.prop)
    return dtype


@load.register(Context, Object, dict, Manifest)
def load(context: Context, dtype: Object, data: dict, manifest: Manifest) -> DataType:
    return _load_properties(context, dtype, manifest)


def _load_properties(context: Context, dtype: DataType, manifest: Manifest) -> DataType:
    props = {}
    for name, params in (dtype.properties or {}).items():
        place = f"{dtype.prop.place}.{name}"
        prop = dtype.prop.__class__()
        prop.name = name
        prop.place = place
        prop.parent = dtype.prop
        prop.model = dtype.prop.model
        prop.list = dtype.prop.list
        commands.load(context, prop, params, manifest)
        dtype.prop.model.flatprops[place] = prop
        props[name] = prop
    dtype.properties = props
    return dtype


@load.register(Context, Array, dict, Manifest)
def load(context: Context, dtype: Array, data: dict, manifest: Manifest) -> DataType:
    return _load_array(context, dtype, manifest)


@load.register(Context, ArrayBackRef, dict, Manifest)
def load(context: Context, dtype: ArrayBackRef, data: dict, manifest: Manifest) -> DataType:
    if dtype.items:
        assert isinstance(dtype.items, dict), type(dtype.items)
        prop: Property = dtype.prop.__class__()
        prop.name = dtype.prop.name
        prop.place = dtype.prop.place
        prop.parent = dtype.prop
        prop.model = dtype.prop.model
        commands.load(context, prop, dtype.items, manifest)
        dtype.items = prop
    else:
        dtype.items = None
    return dtype


def _load_array(context: Context, dtype: Array | ArrayBackRef, manifest: Manifest) -> DataType:
    if dtype.items:
        assert isinstance(dtype.items, dict), type(dtype.items)
        prop: Property = dtype.prop.__class__()
        prop.list = dtype.prop
        prop.name = dtype.prop.name
        prop.place = dtype.prop.place
        prop.parent = dtype.prop
        prop.model = dtype.prop.model
        commands.load(context, prop, dtype.items, manifest)
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
def load(
    context: Context,
    dtype: Array,
    value: object,
) -> Union[None, list]:
    return _load_array_to_list(context, dtype, value)


@load.register(Context, ArrayBackRef, object)
def load(
    context: Context,
    dtype: ArrayBackRef,
    value: object,
) -> Union[None, list]:
    return _load_array_to_list(context, dtype, value)


def _load_array_to_list(context: Context, dtype: Array | ArrayBackRef, value: object) -> Union[None, list]:
    # loads value into native python list, including all list items
    value = dtype.load(value)
    if value is None or value is NA:
        return value
    return [
        commands.load(context, dtype.items.dtype, item)
        for item in value
    ]


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


@load.register(Context, Ref, object)
def load(context: Context, dtype: Ref, value: object) -> dict:
    loaded_obj = dtype.load(value)
    # TODO: add better support for dtype.properties load
    # if isinstance(value, dict):
    #     dtype.properties = {
    #         name: load(context, Property(), prop)
    #         for name, prop in value.get('properties', {}).items()
    #     }
    return loaded_obj


@load.register(Context, BackRef, object)
def load(context: Context, dtype: BackRef, value: object) -> dict:
    loaded_obj = dtype.load(value)
    return loaded_obj


@load.register(Context, RQL, str)
def load(context: Context, dtype: RQL, value: str) -> dict:
    rql = spyna.parse(value)
    return rql


@commands.get_error_context.register(DataType)
def get_error_context(dtype: DataType, *, prefix='this'):
    context = commands.get_error_context(dtype.prop, prefix=f'{prefix}.prop')
    context['type'] = 'this.name'
    return context


@commands.rename_metadata.register(Context, dict)
def rename_metadata(context: Context, data: dict) -> dict:
    return data


@load.register(Context, Partial, dict)
def load(context: Context, dtype: Partial, data: dict):
    loaded_obj = dtype.load(data)
    # TODO: add better support for dtype.properties load
    # if isinstance(value, dict):
    #     dtype.properties = {
    #         name: load(context, Property(), prop)
    #         for name, prop in value.get('properties', {}).items()
    #     }
    return loaded_obj
