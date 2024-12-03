import base64
import datetime
import decimal
import pathlib
import uuid
from typing import Any, Tuple
from typing import AsyncIterator
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional

import dateutil
import shapely.geometry.base
from shapely import wkt

from geoalchemy2.elements import WKTElement, WKBElement
from geoalchemy2.shape import to_shape

from spinta import commands
from spinta import exceptions
from spinta.backends.components import Backend
from spinta.backends.components import SelectTree
from spinta.backends.helpers import check_unknown_props
from spinta.backends.helpers import flat_select_to_nested
from spinta.backends.helpers import get_model_reserved_props
from spinta.backends.helpers import get_select_prop_names
from spinta.backends.helpers import select_keys
from spinta.backends.helpers import select_model_props
from spinta.backends.helpers import select_props
from spinta.commands import gen_object_id
from spinta.commands import is_object_id
from spinta.commands import load_operator_value
from spinta.commands import prepare
from spinta.components import Action, UrlParams, page_in_data
from spinta.components import Context
from spinta.components import DataItem
from spinta.components import Model
from spinta.components import Namespace
from spinta.components import Node
from spinta.components import Property
from spinta.core.ufuncs import asttoexpr
from spinta.exceptions import ConflictingValue, RequiredProperty, LangNotDeclared, TooManyLangsGiven, \
    UnableToDetermineRequiredLang, CoordinatesOutOfRange, InheritPropertyValueMissmatch, SRIDNotSetForGeometry, \
    InvalidUuidValue, DirectRefValueUnassignment
from spinta.exceptions import NoItemRevision
from spinta.formats.components import Format
from spinta.manifests.components import Manifest
from spinta.types.datatype import Array, ExternalRef, Inherit, PageType, BackRef, ArrayBackRef, Integer, Boolean
from spinta.types.datatype import Binary
from spinta.types.datatype import DataType
from spinta.types.datatype import Date
from spinta.types.datatype import DateTime
from spinta.types.datatype import File
from spinta.types.datatype import JSON
from spinta.types.datatype import Number
from spinta.types.datatype import Object
from spinta.types.datatype import PrimaryKey
from spinta.types.datatype import Ref
from spinta.types.datatype import String
from spinta.types.datatype import Time
from spinta.types.datatype import UUID
from spinta.types.geometry.components import Geometry
from spinta.types.geometry.helpers import get_crs_bounding_area
from spinta.types.text.components import Text
from spinta.utils.config import asbool
from spinta.utils.encoding import encode_page_values
from spinta.utils.schema import NA
from spinta.utils.schema import NotAvailable


@commands.prepare_for_write.register(Context, Model, Backend, dict)
def prepare_for_write(
    context: Context,
    model: Model,
    backend: Backend,
    patch: Dict[str, Any],
    *,
    action: Action,
    params: UrlParams = None
) -> dict:
    # prepares model's data for storing in a backend
    backend = model.backend
    result = {}
    for name, value in patch.items():
        if not name.startswith('_'):
            prop = model.properties[name]
            if params:
                value = commands.prepare_for_write(context, prop.dtype, backend, value, params)
            else:
                value = commands.prepare_for_write(context, prop.dtype, backend, value)
        result[name] = value
    return result


@commands.prepare_for_write.register(Context, Property, Backend, object)
def prepare_for_write(
    context: Context,
    prop: Property,
    backend: Backend,
    value: Any,
    *,
    action: Action,
    params: UrlParams = None
) -> Any:
    if prop.name in value:
        value[prop.name] = commands.prepare_for_write(
            context,
            prop.dtype,
            prop.dtype.backend,
            value[prop.name],
            params
        )
    return value


@commands.prepare_for_write.register(Context, DataType, Backend, object, UrlParams)
def prepare_for_write(
    context: Context,
    dtype: DataType,
    backend: Backend,
    value: Any,
    params: UrlParams
) -> Any:
    executor = commands.prepare_for_write[Context, type(dtype), type(backend), type(value)]
    result = executor(context, dtype, backend, value)
    return result


@commands.prepare_for_write.register(Context, DataType, Backend, object)
def prepare_for_write(
    context: Context,
    dtype: DataType,
    backend: Backend,
    value: Any,
) -> Any:
    # prepares value for data store
    # for simple types - loaded native values should work
    # otherwise - override for this command if necessary
    return value


@commands.prepare_for_write.register(Context, Array, Backend, list)
def prepare_for_write(
    context: Context,
    dtype: Array,
    backend: Backend,
    value: List[Any],
) -> List[Any]:
    # prepare array and it's items for datastore
    return [
        commands.prepare_for_write(context, dtype.items.dtype, backend, v)
        for v in value
    ]


@commands.prepare_for_write.register(Context, ArrayBackRef, Backend, list)
def prepare_for_write(
    context: Context,
    dtype: ArrayBackRef,
    backend: Backend,
    value: List[Any],
) -> List[Any]:
    return [
        commands.prepare_for_write(context, dtype.items.dtype, backend, v)
        for v in value
    ]


@commands.prepare_for_write.register(Context, Object, Backend, dict)
def prepare_for_write(
    context: Context,
    dtype: Object,
    backend: Backend,
    value: Dict[str, Any],
) -> Dict[str, Any]:
    # prepare objects and it's properties for datastore
    prepped = {}
    for k, v in value.items():
        prop = dtype.properties[k]
        prepped[k] = commands.prepare_for_write(context, prop.dtype, backend, v)
    return prepped


@commands.prepare_for_write.register(Context, Text, Backend, str, UrlParams)
def prepare_for_write(
    context: Context,
    dtype: Text,
    backend: Backend,
    value: str,
    params: UrlParams
) -> Any:
    default_langs = context.get('config').languages
    preferred_lang = None
    # First Step: Check Content-Language if lang exists in the property
    if params.content_langs:
        if len(params.content_langs) > 1:
            raise TooManyLangsGiven(dtype, amount=len(params.content_langs))

        if params.content_langs[0] not in dtype.langs:
            raise LangNotDeclared(dtype, lang=params.content_langs[0])

        preferred_lang = dtype.langs[params.content_langs[0]]

    # Second Step: if Content-Language not given check default languages
    if not preferred_lang:
        for lang in default_langs:
            if lang in dtype.langs:
                preferred_lang = dtype.langs[lang]
                break

    # Third Step: if Content-Language and default language does not exist, check for C language (unknown)
    if not preferred_lang:
        if not commands.identifiable(dtype.prop) and 'C' in dtype.langs:
            preferred_lang = dtype.langs['C']

    # Fourth Step: if nothing works raise Exception that language was not determined
    if not preferred_lang:
        raise UnableToDetermineRequiredLang()

    value = {preferred_lang.name: value}
    executor = commands.prepare_for_write[Context, Text, type(backend), dict]
    result = executor(context, dtype, backend, value)
    return result


@commands.prepare_for_write.register(Context, Text, Backend, dict, UrlParams)
def prepare_for_write(
    context: Context,
    dtype: Text,
    backend: Backend,
    value: dict,
    params: UrlParams
) -> Any:
    if '' in value:
        value['C'] = value.pop('')
    return value


@prepare.register(Context, Backend, Property)
def prepare(context: Context, backend: Backend, prop: Property, **kwargs):
    return prepare(context, backend, prop.dtype, **kwargs)


@commands.simple_data_check.register(Context, DataItem, Model, Backend)
def simple_data_check(
    context: Context,
    data: DataItem,
    model: Model,
    backend: Backend,
) -> None:
    simple_model_properties_check(context, data, model, backend)


@commands.simple_data_check.register(Context, DataItem, Property, Backend)
def simple_data_check(
    context: Context,
    data: DataItem,
    prop: Property,
    backend: Backend,
) -> None:
    simple_data_check(
        context,
        data,
        prop.dtype,
        prop,
        backend,
        data.given[prop.name],
    )


def simple_model_properties_check(
    context: Context,
    data: DataItem,
    model: Model,
    backend: Backend,
) -> None:
    for name, prop in model.properties.items():
        simple_data_check(
            context,
            data,
            prop.dtype,
            prop,
            prop.dtype.backend,
            data.given.get(name, NA),
        )


@commands.simple_data_check.register(Context, DataItem, DataType, Property, Backend, object)
def simple_data_check(
    context: Context,
    data: DataItem,
    dtype: DataType,
    prop: Property,
    backend: Backend,
    value: object,
) -> None:
    if data.action in (
        Action.UPDATE,
        Action.INSERT,
        Action.PATCH,
        Action.UPSERT
    ):
        check_type_value(dtype, value, data.action)

    # Action.DELETE is ignore for qvarn compatibility reasons.
    # XXX: make `spinta` check for revision on Action.DELETE,
    #      implementers can override this command as they please.
    updating = data.action in (Action.UPDATE, Action.PATCH)
    if updating and '_revision' not in data.given:
        raise NoItemRevision(prop)


@commands.simple_data_check.register(Context, DataItem, UUID, Property, Backend, str)
def simple_data_check(
    context: Context,
    data: DataItem,
    dtype: UUID,
    prop: Property,
    backend: Backend,
    value: str,
) -> None:
    if not isinstance(value, uuid.UUID):
        try:
            uuid.UUID(str(value))
        except ValueError:
            raise InvalidUuidValue(prop, value=value)


@commands.simple_data_check.register(Context, DataItem, Object, Property, Backend, dict)
def simple_data_check(
    context: Context,
    data: DataItem,
    dtype: Object,
    prop: Property,
    backend: Backend,
    value: Dict[str, Any],
) -> None:
    for prop in dtype.properties.values():
        v = value.get(prop.name, NA)
        simple_data_check(context, data, prop.dtype, prop, prop.dtype.backend, v)


@commands.simple_data_check.register(Context, DataItem, Object, Property, Backend, dict)
def simple_data_check(
    context: Context,
    data: DataItem,
    dtype: Object,
    prop: Property,
    backend: Backend,
    value: Dict[str, Any],
) -> None:
    for prop in dtype.properties.values():
        v = value.get(prop.name, NA)
        simple_data_check(context, data, prop.dtype, prop, prop.dtype.backend, v)


@commands.simple_data_check.register(Context, DataItem, Text, Property, Backend, dict)
def simple_data_check(
    context: Context,
    data: DataItem,
    dtype: Text,
    prop: Property,
    backend: Backend,
    value: dict,
) -> None:
    langs = dtype.langs
    for lang, val in value.items():
        if lang not in langs:
            if lang == '' and 'C' in langs:
                continue
            raise LangNotDeclared(dtype, lang=lang)


@commands.simple_data_check.register(Context, DataItem, ExternalRef, Property, Backend, dict)
def simple_data_check(
    context: Context,
    data: DataItem,
    dtype: ExternalRef,
    prop: Property,
    backend: Backend,
    value: dict,
) -> None:
    if value is None:
        return

    denorm_prop_keys = [
        key for key in dtype.properties.keys()
    ]

    if dtype.model.given.pkeys or dtype.explicit:
        allowed_keys = [prop.name for prop in dtype.refprops]
    else:
        allowed_keys = ['_id']

    allowed_keys.extend(denorm_prop_keys)
    for key in value.keys():
        if key not in allowed_keys:
            raise exceptions.FieldNotInResource(prop, property=key)
        elif key in denorm_prop_keys:
            commands.simple_data_check(
                context,
                data,
                dtype.properties[key].dtype,
                dtype.properties[key],
                backend,
                value[key]
            )

    if value and isinstance(value, dict):
        if all(val is None for key, val in value.items() if key in allowed_keys):
            raise DirectRefValueUnassignment(dtype)


@commands.simple_data_check.register(Context, DataItem, Ref, Property, Backend, object)
def simple_data_check(
    context: Context,
    data: DataItem,
    dtype: Ref,
    prop: Property,
    backend: Backend,
    value: object,
) -> None:
    if value is None:
        return

    if value and not isinstance(value, dict):
        raise exceptions.InvalidRefValue(prop, value=value)

    if isinstance(value, dict) and '_id' in value and value['_id'] is None:
        raise DirectRefValueUnassignment(dtype)


@commands.simple_data_check.register(Context, DataItem, BackRef, Property, Backend, object)
def simple_data_check(
    context: Context,
    data: DataItem,
    dtype: BackRef,
    prop: Property,
    backend: Backend,
    value: object,
) -> None:
    if value is not NA:
        raise exceptions.CannotModifyBackRefProp(dtype)
    

@commands.simple_data_check.register(Context, DataItem, ArrayBackRef, Property, Backend, object)
def simple_data_check(
    context: Context,
    data: DataItem,
    dtype: ArrayBackRef,
    prop: Property,
    backend: Backend,
    value: object,
) -> None:
    if value is not NA:
        raise exceptions.CannotModifyBackRefProp(dtype)


@commands.simple_data_check.register(Context, DataItem, Geometry, Property, Backend, str)
def simple_data_check(
    context: Context,
    data: DataItem,
    dtype: Geometry,
    prop: Property,
    backend: Backend,
    value: str,
) -> None:
    if value:
        if ';' in value:
            shape = shapely.wkt.loads(value.split(';', 1)[1])
        else:
            shape = shapely.wkt.loads(value)

        srid = dtype.srid

        if srid is None:
            raise SRIDNotSetForGeometry(dtype)

        bounding_area = get_crs_bounding_area(srid)

        if not bounding_area.contains(shape):
            raise CoordinatesOutOfRange(dtype, given=value, srid=srid, bounds=bounding_area.bounds)


@commands.complex_data_check.register(Context, DataItem, Model, Backend)
def complex_data_check(
    context: Context,
    data: DataItem,
    model: Model,
    backend: Backend,
) -> None:
    # XXX: Maybe Action.DELETE should also provide revision.
    updating = data.action in (Action.UPDATE, Action.PATCH)
    if updating and '_revision' not in data.given:
        raise NoItemRevision(model)

    if data.action in (Action.UPDATE, Action.PATCH, Action.DELETE):
        for k in ('_type', '_revision'):
            if k in data.given and data.saved[k] != data.given[k]:
                raise ConflictingValue(
                    model.properties[k],
                    given=data.given[k],
                    expected=data.saved[k],
                )

    complex_model_properties_check(context, model, backend, data)


def complex_model_properties_check(
    context: Context,
    model: Model,
    backend: Backend,
    data: DataItem,
) -> None:
    base_data = {}
    if model.base and any(isinstance(prop.dtype, Inherit) for prop in model.properties.values()):
        id_ = data.given.get('_id', None)
        if id_ is not None:
            where = {
                'name': 'eq',
                'args': [
                    {'name': 'bind', 'args': ['_id']},
                    id_,
                ]
            }
            query = asttoexpr(where)
            result = commands.getall(context, model.base.parent, backend, query=query)
            for row in result:
                base_data = row
                break

    for name, prop in model.properties.items():
        # For datasets, property type is optional.
        # XXX: But I think, it should be mandatory.
        if prop.dtype is not None:
            given = data.given.get(name, NA)

            if prop.dtype.unique and given is not NA:
                commands.check_unique_constraint(
                    context,
                    data,
                    prop.dtype,
                    prop,
                    data.backend,
                    given,
                )
            complex_data_check(
                context,
                data,
                prop.dtype,
                prop,
                data.backend,
                given,
                base_data=base_data
            )


@complex_data_check.register(Context, DataItem, DataType, Property, Backend, object)
def complex_data_check(
    context: Context,
    data: DataItem,
    dtype: DataType,
    prop: Property,
    backend: Backend,
    value: object,
    **kwargs
):
    if data.action in (Action.UPDATE, Action.PATCH, Action.DELETE):
        for k in ('_type', '_revision'):
            if k in data.given and data.saved[k] != data.given[k]:
                raise ConflictingValue(
                    dtype,
                    given=data.given[k],
                    expected=data.saved[k],
                )


@complex_data_check.register(Context, DataItem, Inherit, Property, Backend, object)
def complex_data_check(
    context: Context,
    data: DataItem,
    dtype: Inherit,
    prop: Property,
    backend: Backend,
    value: object,
    base_data: dict = {},
    **kwargs
):
    if data.action in (Action.UPDATE, Action.PATCH, Action.DELETE):
        for k in ('_type', '_revision'):
            if k in data.given and data.saved[k] != data.given[k]:
                raise ConflictingValue(
                    dtype,
                    given=data.given[k],
                    expected=data.saved[k],
                )

    if value is not NA and dtype.prop.name in base_data:
        inherited_value = base_data[dtype.prop.name]
        if inherited_value != value:
            raise InheritPropertyValueMissmatch(dtype, expected=inherited_value, given=value)


def check_type_value(dtype: DataType, value: object, action: Action):
    if dtype.required and (
        (action == Action.PATCH and value is None) or
        (action != Action.PATCH and (value is None or value is NA))
    ):
        raise RequiredProperty(dtype.prop)


@gen_object_id.register(Context, Backend, Node)
def gen_object_id(context: Context, backend: Backend, model: Node):
    return str(uuid.uuid4())


@gen_object_id.register(Context, Backend)
def gen_object_id(context: Context, backend: Backend):
    return str(uuid.uuid4())


@is_object_id.register(Context, str)
def is_object_id(context: Context, value: str):
    # Collect all available backend/model combinations.
    # TODO: this probably should be cached at load time to increase performance
    #       of object id lookup.
    # XXX: other option would be to implement two pass URL parsing, to get
    #      information about model, and then call is_object_id directly with
    #      backend and model. That would be nice.
    store = context.get('store')
    candidates = set()
    manifest = store.manifest
    for model in manifest.objects.get('model', {}).values():
        candidates.add((model.backend.__class__, model.__class__))

    # Currently all models use UUID, but dataset models use id generated from
    # other properties that form an unique id.
    for Backend_, Model_ in candidates:
        backend = Backend_()
        model = Model_()
        model.name = ''
        if is_object_id(context, backend, model, value):
            return True


@is_object_id.register(Context, Backend, Model, str)
def is_object_id(context: Context, backend: Backend, model: Model, value: str):
    try:
        return uuid.UUID(value).version == 4
    except ValueError:
        return False


@is_object_id.register(Context, Backend, Model, object)
def is_object_id(context: Context, backend: Backend, model: Model, value: object):
    # returns false if object id is non string.
    # strings are handled by other command
    return False


@is_object_id.register(Context, type(None), Namespace, object)
def is_object_id(context: Context, backend: type(None), model: Namespace, value: object):
    # Namespaces do not have object id.
    return False


@commands.prepare_data_for_response.register(Context, Namespace, Format, dict)
def prepare_data_for_response(
    context: Context,
    ns: Namespace,
    fmt: Format,
    value: dict,
    *,
    action: Action,
    select: SelectTree,
    prop_names: List[str],
) -> dict:
    return {
        prop.name: commands.prepare_dtype_for_response(
            context,
            fmt,
            prop.dtype,
            val,
            data=value,
            action=action,
            select=sel,
        )
        for prop, val, sel in select_model_props(
            commands.get_model(context, ns.manifest, '_ns'),
            prop_names,
            value,
            select,
            reserved=['_id'],
        )
    }


@commands.prepare_data_for_response.register(Context, Model, Format, dict)
def prepare_data_for_response(
    context: Context,
    model: Model,
    fmt: Format,
    value: dict,
    *,
    action: Action,
    select: SelectTree,
    prop_names: List[str],
) -> dict:
    return {
        prop.name: commands.prepare_dtype_for_response(
            context,
            fmt,
            prop.dtype,
            val,
            data=value,
            action=action,
            select=sel,
        )
        for prop, val, sel in select_model_props(
            model,
            prop_names,
            value,
            select,
            get_model_reserved_props(action, page_in_data(value)),
        )
    }


@commands.prepare_data_for_response.register(Context, Object, Format, dict)
def prepare_data_for_response(
    context: Context,
    dtype: Object,
    fmt: Format,
    value: dict,
    *,
    action: Action,
    select: List[str] = None,
) -> dict:
    select = flat_select_to_nested(select)
    return {
        prop.name: commands.prepare_dtype_for_response(
            context,
            fmt,
            prop.dtype,
            value,
            data=value,
            action=action,
            select=select,
        )
        for prop, value, select in _select_prop_props(
            dtype,
            value,
            select,
            ['_type', '_revision'],
        )
    }


@commands.prepare_data_for_response.register(Context, File, Format, dict)
def prepare_data_for_response(
    context: Context,
    dtype: File,
    fmt: Format,
    value: dict,
    *,
    action: Action,
    select: List[str] = None,
    property_: Property = None,
) -> dict:
    prop = dtype.prop
    reserved = ['_type', '_revision']

    if prop.name in value and value[prop.name]:
        check_unknown_props(prop, select, set(reserved) | set(value[prop.name]))
    else:
        check_unknown_props(prop, select, set(reserved))

    if prop.name in value and value[prop.name]:
        data = commands.prepare_dtype_for_response(
            context,
            fmt,
            prop.dtype,
            value[prop.name],
            data=value,
            action=action,
            select=select,
        )
    else:
        data = {}

    for key, val, sel in select_keys(reserved, value, select):
        data[key] = val

    return data


def _select_prop_props(
    dtype: Object,
    value: dict,
    select: SelectTree,
    reserved: List[str],
):
    prop = dtype.prop
    if prop.name in value:
        check_unknown_props(prop, select, set(reserved) | set(value[prop.name]))
    else:
        check_unknown_props(prop, select, set(reserved))
    yield from select_props(
        prop,
        reserved,
        prop.model.properties,
        value,
        select,
    )
    if prop.name in value:
        yield from select_props(
            prop,
            value[prop.name].keys(),
            dtype.properties,
            value[prop.name],
            select,
        )

@commands.prepare_dtype_for_response.register(Context, Format, DataType, object)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: DataType,
    value: Any,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    assert isinstance(value, (str, int, float, bool, type(None), list, dict)), (
        f"prepare_dtype_for_response must return only primitive, json "
        f"serializable types, {type(value)} is not a primitive data type, "
        f"model={dtype.prop.model!r}, dtype={dtype!r}"
    )
    return value


@commands.prepare_dtype_for_response.register(Context, Format, DataType, NotAvailable)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: DataType,
    value: NotAvailable,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return dtype.default


@commands.prepare_dtype_for_response.register(Context, Format, UUID, object)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: UUID,
    value: object,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return str(value)

@commands.prepare_dtype_for_response.register(Context, Format, File, NotAvailable)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: File,
    value: NotAvailable,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return {
        '_id': None,
        '_content_type': None,
    }


@commands.prepare_dtype_for_response.register(Context, Format, File, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: File,
    value: dict,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    data = {
        key: val
        for key, val, sel in select_keys(
            ['_id', '_content_type'],
            value,
            select,
        )
    }

    if isinstance(data.get('_id'), pathlib.Path):
        # _id on FileSystem backend is a Path.
        data['_id'] = str(data['_id'])

    # File content is returned only if explicitly requested.
    if select and '_content' in select:
        if '_content' in value:
            content = value['_content']
        else:
            prop = dtype.prop
            content = commands.getfile(context, prop, dtype, dtype.backend, data=value)

        if content is None:
            data['_content'] = None
        else:
            data['_content'] = base64.b64encode(content).decode()

    return data


@commands.prepare_dtype_for_response.register(Context, Format, DateTime, datetime.datetime)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: DateTime,
    value: datetime.datetime,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return value.isoformat()


@commands.prepare_dtype_for_response.register(Context, Format, Date, datetime.date)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: Date,
    value: datetime.date,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    else:
        return value.isoformat()


@commands.prepare_dtype_for_response.register(Context, Format, Time, datetime.time)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: Time,
    value: datetime.time,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return value.isoformat()


@commands.prepare_dtype_for_response.register(Context, Format, Binary, bytes)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: Binary,
    value: bytes,
    data: Dict[str, Any],
    *,
    action: Action,
    select: dict = None,
):
    return base64.b64encode(value).decode('ascii')


@commands.prepare_dtype_for_response.register(Context, Format, Geometry, shapely.geometry.base.BaseGeometry)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: Geometry,
    value: shapely.geometry.base.BaseGeometry,
    data: Dict[str, Any],
    *,
    action: Action,
    select: dict = None,
):
    return value.wkt


@commands.prepare_dtype_for_response.register(Context, Format, PageType, list)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: PageType,
    value: list,
    data: Dict[str, Any],
    *,
    action: Action,
    select: dict = None,
):
    return encode_page_values(value).decode('ascii')


@commands.prepare_dtype_for_response.register(Context, Format, Ref, (dict, type(None)))
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: Ref,
    value: Optional[Dict[str, Any]],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    if value is None:
        return None

    properties = dtype.model.properties.copy()
    properties.update(dtype.properties)

    nullable = True
    if select and select != {'*': {}}:
        nullable = False
        names = get_select_prop_names(
            context,
            dtype,
            properties,
            action,
            select,
            reserved=['_id'],
        )
    else:
        names = value.keys()

    result = {
        prop.name: commands.prepare_dtype_for_response(
            context,
            fmt,
            prop.dtype,
            val,
            data=data,
            action=action,
            select=sel,
        )
        for prop, val, sel in select_props(
            dtype.model,
            names,
            properties,
            value,
            select,
        )
    }

    # Apply None checks at the end, since there might be nested values that are only calculated at the end
    # There are some cases where data changelog was not logging ref unassignment correctly and would store values as
    # empty string, instead of null
    # https://github.com/atviriduomenys/spinta/issues/556
    if nullable and all(val is None or val == "" for val in result.values()):
        return None

    return result


@commands.prepare_dtype_for_response.register(Context, Format, Ref, str)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: Ref,
    value: str,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    # FIXME: Backend should never return references as strings! References
    #        should always be dicts.

    # This is a workaround for `"value": ""` cases, where they should have been `"value": null`
    # https://github.com/atviriduomenys/spinta/issues/556
    if value == "":
        return None

    return {'_id': value}


@commands.prepare_dtype_for_response.register(Context, Format, ExternalRef, (dict, type(None)))
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: ExternalRef,
    value: Optional[Dict[str, Any]],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    if value is None:
        return None

    nullable = True
    if select and select != {'*': {}}:
        nullable = False
        names = get_select_prop_names(
            context,
            dtype,
            dtype.model.properties,
            action,
            select,
        )
    else:
        names = value.keys()

    result = {}
    props = dtype.model.properties.copy()
    props.update(dtype.properties)

    for prop, val, sel in select_props(
        dtype.model,
        names,
        props,
        value,
        select,
    ):
        result[prop.name] = commands.prepare_dtype_for_response(
            context,
            fmt,
            prop.dtype,
            val,
            data=value,
            action=action,
            select=sel,
        )

    # Apply None checks at the end, since there might be nested values that are only calculated at the end
    if nullable and all(val is None for val in result.values()):
        return None

    return result


@commands.prepare_dtype_for_response.register(Context, Format, Object, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: Object,
    value: dict,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    names = get_select_prop_names(
        context,
        dtype,
        dtype.properties,
        action,
        select,
        # XXX: Not sure if auth=False should be here. But I'm too tiered to
        #      check why it does not work.
        auth=False,
    )
    return {
        prop.name: commands.prepare_dtype_for_response(
            context,
            fmt,
            prop.dtype,
            val,
            data=data,
            action=action,
            select=sel,
        )
        for prop, val, sel in select_props(
            dtype.prop,
            names,
            dtype.properties,
            value,
            select,
        )
    }


@commands.prepare_dtype_for_response.register(Context, Format, Array, list)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: Array,
    value: list,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
) -> list:
    return _prepare_array_for_response(
        context,
        dtype,
        fmt,
        value,
        data,
        action,
        select,
    )


@commands.prepare_dtype_for_response.register(Context, Format, Array, tuple)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: Array,
    value: tuple,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
) -> list:
    return _prepare_array_for_response(
        context,
        dtype,
        fmt,
        value,
        data,
        action,
        select,
    )


@commands.prepare_dtype_for_response.register(Context, Format, DateTime, datetime.datetime)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: DateTime,
    value: datetime.datetime,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return value.isoformat()


def _prepare_array_for_response(
    context: Context,
    dtype: Array,
    fmt: Format,
    value: Iterable,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
) -> list:
    if dtype.items:
        return [
            commands.prepare_dtype_for_response(
                context,
                fmt,
                dtype.items.dtype,
                v,
                data=data,
                action=action,
                select=select,
            )
            for v in value
        ]
    else:
        return [v for v in value]


@commands.prepare_dtype_for_response.register(Context, Format, Array, type(None))
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: Array,
    value: type(None),
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return []


@commands.prepare_dtype_for_response.register(Context, Format, JSON, object)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: JSON,
    value: object,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return value


@commands.prepare_dtype_for_response.register(Context, Format, JSON, NotAvailable)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: JSON,
    value: NotAvailable,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return None


@commands.prepare_dtype_for_response.register(Context, Format, Number, decimal.Decimal)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: Number,
    value: decimal.Decimal,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return float(value)


@commands.prepare_dtype_for_response.register(Context, Format, Inherit, object)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: DataType,
    value: Any,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    base_model = get_property_base_model(dtype.prop.model, dtype.prop.name)
    if base_model:
        prop = base_model.properties[dtype.prop.name]
        return commands.prepare_dtype_for_response(
            context,
            fmt,
            prop.dtype,
            value,
            data=data,
            action=action,
            select=select
        )
    return None


@commands.prepare_dtype_for_response.register(Context, Format, Inherit, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: DataType,
    value: Any,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    if dtype.prop.name == '_base' and value:
        data = {}
        for name in value.keys():
            base_model = get_property_base_model(dtype.prop.model, name)
            if base_model:
                data.update({
                    prop.name: commands.prepare_dtype_for_response(
                        context,
                        fmt,
                        prop.dtype,
                        val,
                        data=data,
                        action=action,
                        select=sel,
                    )
                    for prop, val, sel in select_props(
                        base_model,
                        [name],
                        base_model.properties,
                        value,
                        select,
                    )
                })
        return data
    return {}


@commands.prepare_dtype_for_response.register(Context, Format, Inherit, NotAvailable)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: Inherit,
    value: NotAvailable,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return None


@commands.prepare_dtype_for_response.register(Context, Format, ArrayBackRef, (list, tuple))
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: ArrayBackRef,
    value: (list, tuple),
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
) -> list:
    return _prepare_array_for_response(
        context,
        dtype,
        fmt,
        value,
        data,
        action,
        select,
    )


@commands.prepare_dtype_for_response.register(Context, Format, ArrayBackRef, type(None))
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: ArrayBackRef,
    value: type(None),
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
) -> list:
    return []


@commands.prepare_dtype_for_response.register(Context, Format, BackRef, (dict, type(None)))
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: BackRef,
    value: Optional[Dict[str, Any]],
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    if value is None or all(val is None for val in value.values()):
        return None

    if select and select != {'*': {}}:
        names = get_select_prop_names(
            context,
            dtype,
            dtype.model.properties,
            action,
            select,
            reserved=['_id'],
        )
    else:
        names = value.keys()

    data = {
        prop.name: commands.prepare_dtype_for_response(
            context,
            fmt,
            prop.dtype,
            val,
            data=data,
            action=action,
            select=sel,
        )
        for prop, val, sel in select_props(
            dtype.model,
            names,
            dtype.model.properties,
            value,
            select,
        )
    }

    return data


@commands.prepare_dtype_for_response.register(Context, Format, BackRef, str)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: BackRef,
    value: str,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    return {'_id': value}


@commands.prepare_dtype_for_response.register(Context, Format, Text, dict)
def prepare_dtype_for_response(
    context: Context,
    fmt: Format,
    dtype: Text,
    value: dict,
    *,
    data: Dict[str, Any],
    action: Action,
    select: dict = None,
):
    if len(value) == 1 and select is not None:
        for key, data in value.items():
            if key not in select.keys():
                return data
    return value


def get_property_base_model(model: Model, name: str):
    model = model
    base_model = None
    while model.base and model.base.parent:
        model = model.base.parent
        if name in model.properties and not isinstance(
            model.properties[name].dtype,
            Inherit
        ):
            base_model = model
            break
    return base_model


@commands.unload_backend.register(Context, Backend)
def unload_backend(context: Context, backend: Backend):
    pass


@load_operator_value.register(Context, Backend, DataType, object)
def load_operator_value(
    context: Context,
    backend: Backend,
    dtype: DataType,
    value: object,
    *,
    query_params: dict,
):
    operator = query_params['name']
    # XXX: That is probably not a very reliable way of getting original operator
    #      name. Maybe at least this should be documented some how.
    operator_name = query_params.get('origin', operator)
    if operator in ('startswith', 'contains') and not isinstance(dtype, (String, PrimaryKey)):
        raise exceptions.InvalidOperandValue(dtype, operator=operator_name)
    if operator in ('gt', 'ge', 'lt', 'le') and isinstance(dtype, String):
        raise exceptions.InvalidOperandValue(dtype, operator=operator_name)


@commands.make_json_serializable.register(Model, dict)
def make_json_serializable(model: Model, value: dict) -> dict:
    return commands.make_json_serializable[Object, dict](model, value)


@commands.make_json_serializable.register(DataType, object)
def make_json_serializable(dtype: DataType, value: object):
    return value


@commands.make_json_serializable.register(DateTime, datetime.datetime)
def make_json_serializable(dtype: DateTime, value: datetime.datetime):
    return value.isoformat()


@commands.make_json_serializable.register(Date, datetime.date)
def make_json_serializable(dtype: Date, value: datetime.date):
    return value.isoformat()


@commands.make_json_serializable.register(Date, datetime.datetime)
def make_json_serializable(dtype: Date, value: datetime.datetime):
    return value.date().isoformat()


def _get_json_serializable_props(dtype: Object, value: dict):
    for k, v in value.items():
        if k in dtype.properties:
            yield dtype.properties[k], v
        else:
            Exception(f"Unknown {k} key in {dtype!r} object.")


@commands.make_json_serializable.register(Object, dict)
def make_json_serializable(dtype: Object, value: dict):
    return {
        prop.name: commands.make_json_serializable(prop.dtype, v)
        for prop, v in _get_json_serializable_props(dtype, value)
    }


@commands.make_json_serializable.register(Array, list)
def make_json_serializable(dtype: Array, value: list):
    return [make_json_serializable(dtype.items.dtype, v) for v in value]


@commands.update.register(Context, Property, DataType, Backend)
def update(
    context: Context,
    prop: Property,
    dtype: DataType,
    backend: Backend,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    return commands.update(
        context, prop.model, prop.model.backend,
        dstream=dstream,
        stop_on_error=stop_on_error,
    )


@commands.patch.register(Context, Property, DataType, Backend)
def patch(
    context: Context,
    prop: Property,
    dtype: DataType,
    backend: Backend,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    return commands.update(
        context, prop.model, prop.model.backend,
        dstream=dstream,
        stop_on_error=stop_on_error,
    )


@commands.delete.register(Context, Property, DataType, Backend)
def delete(
    context: Context,
    prop: Property,
    dtype: DataType,
    backend: Backend,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    dstream = _prepare_property_for_delete(prop, dstream)
    return commands.update(
        context, prop.model, prop.model.backend,
        dstream=dstream,
        stop_on_error=stop_on_error,
    )


async def _prepare_property_for_delete(
    prop: Property,
    dstream: AsyncIterator[DataItem],
) -> AsyncIterator[DataItem]:
    async for data in dstream:
        if data.patch:
            data.patch = {
                **{k: v for k, v in data.patch.items() if k.startswith('_')},
                prop.name: None,
            }
        yield data


@commands.cast_backend_to_python.register(Context, Model, Backend, dict)
def cast_backend_to_python(
    context: Context,
    model: Model,
    backend: Backend,
    data: dict,
) -> dict:
    return {
        k: commands.cast_backend_to_python(
            context,
            model.properties[k].dtype,
            backend,
            v,
        ) if k in model.properties else v
        for k, v in data.items()
    }


@commands.cast_backend_to_python.register(Context, Property, Backend, object)
def cast_backend_to_python(
    context: Context,
    prop: Property,
    backend: Backend,
    data: object,
) -> dict:
    return commands.cast_backend_to_python(context, prop.dtype, backend, data)


@commands.cast_backend_to_python.register(Context, DataType, Backend, object)
def cast_backend_to_python(
    context: Context,
    dtype: DataType,
    backend: Backend,
    data: Any,
) -> Any:
    if _check_if_nan(data):
        return None
    return data


@commands.cast_backend_to_python.register(Context, UUID, Backend, object)
def cast_backend_to_python(
    context: Context,
    dtype: UUID,
    backend: Backend,
    data: Any,
) -> Any:
    if _check_if_nan(data):
        return None
    if isinstance(data, str):
        try:
            return uuid.UUID(str(data))
        except:
            return data
    return data


@commands.cast_backend_to_python.register(Context, DateTime, Backend, object)
def cast_backend_to_python(
    context: Context,
    dtype: DateTime,
    backend: Backend,
    data: Any,
) -> Any:
    if _check_if_nan(data):
        return None
    if isinstance(data, str):
        try:
            return dateutil.parser.isoparse(data)
        except:
            return data
    return data


@commands.cast_backend_to_python.register(Context, Time, Backend, object)
def cast_backend_to_python(
    context: Context,
    dtype: Time,
    backend: Backend,
    data: Any,
) -> Any:
    if _check_if_nan(data):
        return None
    if isinstance(data, str) and ":" in data:
        try:
            isoparser = dateutil.parser.isoparser()
            return isoparser.parse_isotime(data)
        except:
            return data
    return data


@commands.cast_backend_to_python.register(Context, Date, Backend, object)
def cast_backend_to_python(
    context: Context,
    dtype: Date,
    backend: Backend,
    data: Any,
) -> Any:
    if _check_if_nan(data):
        return None
    if isinstance(data, str):
        try:
            isoparser = dateutil.parser.isoparser()
            return isoparser.parse_isodate(data)
        except:
            return data
    return data


@commands.cast_backend_to_python.register(Context, Integer, Backend, object)
def cast_backend_to_python(
    context: Context,
    dtype: Integer,
    backend: Backend,
    data: Any,
) -> Any:
    if _check_if_nan(data):
        return None
    if isinstance(data, str):
        try:
            new = data
            if "," in data and "." not in data and data.count(",") == 1:
                new = data.replace(",", ".")
            elif "," in data:
                new = data.replace(",", "")
            return int(new)
        except:
            return data
    return data


@commands.cast_backend_to_python.register(Context, Number, Backend, object)
def cast_backend_to_python(
    context: Context,
    dtype: Number,
    backend: Backend,
    data: Any,
) -> Any:
    if _check_if_nan(data):
        return None
    if isinstance(data, str):
        try:
            new = data
            if "," in data and "." not in data and data.count(",") == 1:
                new = data.replace(",", ".")
            elif "," in data:
                new = data.replace(",", "")
            return float(new)
        except:
            return data
    return data


@commands.cast_backend_to_python.register(Context, Binary, Backend, str)
def cast_backend_to_python(
    context: Context,
    dtype: Binary,
    backend: Backend,
    data: str,
) -> Any:
    if _check_if_nan(data):
        return None
    if isinstance(data, str):
        try:
            return base64.b64decode(data)
        except Exception as e:
            return data
    return data


@commands.cast_backend_to_python.register(Context, Boolean, Backend, str)
def cast_backend_to_python(
    context: Context,
    dtype: Boolean,
    backend: Backend,
    data: str,
) -> Any:
    if _check_if_nan(data):
        return None
    if isinstance(data, str):
        try:
            return asbool(data)
        except:
            return data
    return data


@commands.cast_backend_to_python.register(Context, Geometry, Backend, str)
def cast_backend_to_python(
    context: Context,
    dtype: Geometry,
    backend: Backend,
    data: str,
) -> Any:
    if _check_if_nan(data):
        return None
    if isinstance(data, str):
        try:
            return wkt.loads(data)
        except:
            return data
    return data


@commands.cast_backend_to_python.register(Context, Geometry, Backend, WKTElement)
def cast_backend_to_python(
    context: Context,
    dtype: Geometry,
    backend: Backend,
    data: WKTElement,
) -> Any:
    if _check_if_nan(data):
        return None
    return to_shape(data)


@commands.cast_backend_to_python.register(Context, Geometry, Backend, WKBElement)
def cast_backend_to_python(
    context: Context,
    dtype: Geometry,
    backend: Backend,
    data: WKBElement,
) -> Any:
    if _check_if_nan(data):
        return None
    return to_shape(data)


@commands.cast_backend_to_python.register(Context, Ref, Backend, object)
def cast_backend_to_python(
    context: Context,
    dtype: Ref,
    backend: Backend,
    data: Dict[str, Any],
) -> Any:
    if data:
        result = {}
        for key, value in data.items():
            converted = value
            new_type = None
            for item in dtype.refprops:
                if item.name == key:
                    new_type = item.dtype
                    break
            if new_type:
                converted = commands.cast_backend_to_python(
                    context,
                    new_type,
                    backend,
                    value
                )
            result[key] = converted

        if not result or all(value is None for value in result.values()):
            return None

        return result
    return data


@commands.cast_backend_to_python.register(Context, Object, Backend, dict)
def cast_backend_to_python(
    context: Context,
    dtype: Object,
    backend: Backend,
    data: Dict[str, Any],
) -> Dict[str, Any]:
    if data:
        return {
            k: commands.cast_backend_to_python(
                context,
                dtype.properties[k].dtype,
                backend,
                v,
            ) if k in dtype.properties else v
            for k, v in data.items()
        }
    return data


@commands.cast_backend_to_python.register(Context, Array, Backend, list)
def cast_backend_to_python(
    context: Context,
    dtype: Array,
    backend: Backend,
    data: List[Any],
) -> List[Any]:
    if data and dtype:
        return [
            commands.cast_backend_to_python(
                context,
                dtype.items.dtype,
                backend,
                v,
            )
            for v in data
        ]
    return data


@commands.reload_backend_metadata.register(Context, Manifest, Backend)
def reload_backend_metadata(context, manifest, backend):
    pass


@commands.reload_backend_metadata.register(Context, Manifest, type(None))
def reload_backend_metadata(context, manifest, backend):
    pass


def _check_if_nan(value: Any) -> bool:
    # Check for nan values, IEEE 754 defines that comparing with nan always returns false
    if value != value:
        return True
    return False


@commands.get_error_context.register(Backend)
def get_error_context(backend: Backend, *, prefix='this') -> Dict[str, str]:
    return {
        'type': f'{prefix}.type',
        'name': f'{prefix}.name',
        'origin': f'{prefix}.origin',
        'features': f'{prefix}.features',
    }
