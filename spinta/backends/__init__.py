from typing import AsyncIterator, Union, List, Dict, Optional, Iterable, Generator

import base64
import datetime
import pathlib
import uuid
import typing

from spinta import commands
from spinta import exceptions
from spinta.commands import load_operator_value, prepare, gen_object_id, is_object_id
from spinta.components import Context, Namespace, Model, Property, Action, Node, DataItem
from spinta.exceptions import ConflictingValue, NoItemRevision
from spinta.types.datatype import DataType, DateTime, Date, Object, Array, String, File, PrimaryKey, Binary, Ref, JSON
from spinta.utils.itertools import chunks, recursive_keys
from spinta.utils.schema import NotAvailable, NA
from spinta.backends.components import Backend

SelectTree = Optional[Dict[str, dict]]


@prepare.register()
def prepare(context: Context, dtype: DataType, backend: Backend, value: object) -> object:
    # prepares value for data store
    # for simple types - loaded native values should work
    # otherwise - override for this command if necessary
    return value


@prepare.register()
def prepare(context: Context, dtype: Array, backend: Backend, value: list) -> list:
    # prepare array and it's items for datastore
    return [prepare(context, dtype.items.dtype, backend, v) for v in value]


@prepare.register()
def prepare(context: Context, dtype: Object, backend: Backend, value: dict) -> dict:
    # prepare objects and it's properties for datastore
    new_loaded_obj = {}
    for k, prop in dtype.properties.items():
        # only load value keys which are available in schema
        # FIXME: If k is not in value, then value should be NA. Do not skip a value
        if k in value:
            new_loaded_obj[k] = prepare(context, prop.dtype, backend, value[k])
    return new_loaded_obj


@prepare.register()
def prepare(context: Context, backend: Backend, prop: Property):
    return prepare(context, backend, prop.dtype)


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
    check_type_value(dtype, value)

    updating = data.action in (Action.UPDATE, Action.PATCH)
    if updating and '_revision' not in data.given:
        raise NoItemRevision(prop)


@commands.simple_data_check.register(Context, DataItem, Object, Property, Backend, dict)
def simple_data_check(
    context: Context,
    data: DataItem,
    dtype: DataType,
    prop: Property,
    backend: Backend,
    value: object,
) -> None:
    for prop in dtype.properties.values():
        v = value.get(prop.name, NA)
        simple_data_check(context, data, prop.dtype, prop, prop.dtype.backend, v)


@commands.simple_data_check.register(Context, DataItem, Array, Property, Backend, list)
def simple_data_check(
    context: Context,
    data: DataItem,
    dtype: DataType,
    prop: Property,
    backend: Backend,
    value: list,
) -> None:
    dtype = dtype.items.dtype
    for v in value:
        simple_data_check(context, data, dtype, prop, dtype.backend, v)


@commands.complex_data_check.register()
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
            )


@complex_data_check.register()
def complex_data_check(
    context: Context,
    data: DataItem,
    dtype: DataType,
    prop: Property,
    backend: Backend,
    value: object,
):
    if data.action in (Action.UPDATE, Action.PATCH, Action.DELETE):
        for k in ('_type', '_revision'):
            if k in data.given and data.saved[k] != data.given[k]:
                raise ConflictingValue(
                    dtype,
                    given=data.given[k],
                    expected=data.saved[k],
                )


def check_type_value(dtype: DataType, value: object):
    if dtype.required and (value is None or value is NA):
        # FIXME: Raise a UserError
        raise Exception(f"{dtype.prop.name!r} is required for {dtype.prop.model.name!r}.")


@gen_object_id.register()
def gen_object_id(context: Context, backend: Backend, model: Node):
    return str(uuid.uuid4())


@is_object_id.register()
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


@is_object_id.register()
def is_object_id(context: Context, backend: Backend, model: Model, value: str):
    try:
        return uuid.UUID(value).version == 4
    except ValueError:
        return False


@is_object_id.register()
def is_object_id(context: Context, backend: Backend, model: Model, value: object):
    # returns false if object id is non string.
    # strings are handled by other command
    return False


@is_object_id.register()
def is_object_id(context: Context, backend: type(None), model: Namespace, value: object):
    # Namespaces do not have object id.
    return False


@commands.prepare_data_for_response.register(Context, Action, Model, Backend, dict)
def prepare_data_for_response(  # noqa
    context: Context,
    action: Action,
    model: Model,
    backend: Backend,
    value: dict,
    *,
    select: typing.List[str] = None,
) -> dict:
    select = _apply_always_show_id(context, action, select)
    select = _flat_select_to_nested(select)
    return {
        prop.name: commands.prepare_dtype_for_response(
            context,
            backend,
            model,
            prop.dtype,
            val,
            select=sel,
        )
        for prop, val, sel in _select_model_props(
            model,
            value,
            select,
            ['_type', '_id', '_revision'],
        )
    }


@commands.prepare_data_for_response.register(Context, Action, Object, Backend, dict)
def prepare_data_for_response(  # noqa
    context: Context,
    action: Action,
    dtype: Object,
    backend: Backend,
    value: dict,
    *,
    select: typing.List[str] = None,
) -> dict:
    select = _flat_select_to_nested(select)
    return {
        prop.name: commands.prepare_dtype_for_response(
            context,
            backend,
            prop.model,
            prop.dtype,
            value,
            select=select,
        )
        for prop, value, select in _select_prop_props(
            dtype.prop,
            value,
            select,
            ['_type', '_revision'],
        )
    }


@commands.prepare_data_for_response.register(Context, Action, File, Backend, dict)
def prepare_data_for_response(  # noqa
    context: Context,
    action: Action,
    dtype: File,
    backend: Backend,
    value: dict,
    *,
    select: List[str] = None,
    property_: Property = None,
) -> dict:
    prop = dtype.prop
    reserved = ['_type', '_revision']

    if prop.name in value and value[prop.name]:
        _check_unknown_props(prop, select, set(reserved) | set(value[prop.name]))
    else:
        _check_unknown_props(prop, select, set(reserved))

    if prop.name in value and value[prop.name]:
        data = commands.prepare_dtype_for_response(
            context,
            backend,
            prop.model,
            prop.dtype,
            value[prop.name],
            select=select,
        )
    else:
        data = {}

    for key, val, sel in _select_props(reserved, None, value, select):
        data[key] = val

    return data


def _apply_always_show_id(
    context: Context,
    action: Action,
    select: Optional[List[str]],
) -> Optional[List[str]]:
    if action in (Action.GETALL, Action.SEARCH):
        config = context.get('config')
        if config.always_show_id:
            if select is None:
                return ['_id']
            elif '_id' not in select:
                return ['_id'] + select
    return select


def _select_model_props(
    model: Model,
    value: dict,
    select: SelectTree,
    reserved: List[str],
):
    _check_unknown_props(model, select, set(reserved) | set(value))
    yield from _select_props(
        reserved,
        model.properties,
        value,
        select,
    )
    yield from _select_props(
        value.keys(),
        model.properties,
        value,
        select,
        reserved=False,
    )


def _select_prop_props(
    prop: Property,
    value: dict,
    select: SelectTree,
    reserved: List[str],
):
    if prop.name in value:
        _check_unknown_props(prop, select, set(reserved) | set(value[prop.name]))
    else:
        _check_unknown_props(prop, select, set(reserved))
    yield from _select_props(
        reserved,
        prop.model.properties,
        value,
        select,
    )
    if prop.name in value:
        yield from _select_props(
            value[prop.name].keys(),
            prop.dtype.properties,
            value[prop.name],
            select,
        )


def _select_props(
    keys: List[str],
    props: Optional[Dict[str, Property]],
    value: dict,
    select: SelectTree,
    *,
    reserved: bool = True,
):
    for k in keys:

        if k not in value:
            # Omit all keys if they are not present in value, this is a common
            # case in PATCH requests.
            continue

        if reserved is False and k.startswith('_'):
            continue

        if select is None:
            sel = None
        elif '*' in select:
            sel = select['*']
        elif k in select:
            sel = select[k]
        else:
            continue

        sel = sel or {'*': {}}

        if props is None:
            prop = k
        else:
            prop = props[k]
            if prop.hidden:
                continue

        yield prop, value[k], sel


# FIXME: We should check select list at the very beginning of
#        request, not when returning results.
def _check_unknown_props(
    node: Union[Model, Property, DataType],
    select: Optional[Iterable[str]],
    known: Iterable[str],
):
    unknown_properties = set(select or []) - set(known) - {'*'}
    if unknown_properties:
        raise exceptions.MultipleErrors(
            exceptions.FieldNotInResource(node, property=prop)
            for prop in sorted(unknown_properties)
        )


def _flat_select_to_nested(select: Optional[List[str]]) -> SelectTree:
    """
    >>> _flat_select_to_nested(None)

    >>> _flat_select_to_nested(['foo.bar'])
    {'foo': {'bar': {}}}

    """
    if select is None:
        return None

    res = {}
    for v in select:
        names = v.split('.')
        vref = res
        for name in names:
            if name not in vref:
                vref[name] = {}
            vref = vref[name]

    return res


@commands.prepare_dtype_for_response.register(Context, Backend, Model, DataType, object)
def prepare_dtype_for_response(  # noqa
    context: Context,
    backend: Backend,
    model: Model,
    dtype: DataType,
    value: object,
    *,
    select: dict = None,
):
    assert isinstance(value, (str, int, float, bool, type(None))), (
        f"prepare_dtype_for_response must return only primitive, json "
        f"serializable types, {type(value)} is not a primitive data type, "
        f"model={model!r}, dtype={dtype!r}"
    )
    return value


@commands.prepare_dtype_for_response.register(Context, Backend, Model, DataType, NotAvailable)
def prepare_dtype_for_response(  # noqa
    context: Context,
    backend: Backend,
    model: Model,
    dtype: DataType,
    value: NotAvailable,
    *,
    select: dict = None,
):
    return dtype.prop.default


@commands.prepare_dtype_for_response.register(Context, Backend, Model, File, NotAvailable)
def prepare_dtype_for_response(  # noqa
    context: Context,
    backend: Backend,
    model: Model,
    dtype: File,
    value: NotAvailable,
    *,
    select: dict = None,
):
    return {
        '_id': None,
        '_content_type': None,
    }


@commands.prepare_dtype_for_response.register(Context, Backend, Model, File, dict)
def prepare_dtype_for_response(  # noqa
    context: Context,
    backend: Backend,
    model: Model,
    dtype: File,
    value: dict,
    *,
    select: dict = None,
):
    data = {
        key: val
        for key, val, sel in _select_props(
            ['_id', '_content_type'],
            None,
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


@commands.prepare_dtype_for_response.register(Context, Backend, Model, DateTime, datetime.datetime)
def prepare_dtype_for_response(  # noqa
    context: Context,
    backend: Backend,
    model: Model,
    dtype: DateTime,
    value: datetime.datetime,
    *,
    select: dict = None,
):
    return value.isoformat()


@commands.prepare_dtype_for_response.register(Context, Backend, Model, Date, datetime.date)
def prepare_dtype_for_response(  # noqa
    context: Context,
    backend: Backend,
    model: Model,
    dtype: Date,
    value: datetime.date,
    *,
    select: dict = None,
):
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    else:
        return value.isoformat()


@commands.prepare_dtype_for_response.register(Context, Backend, Model, Binary, bytes)
def prepare_dtype_for_response(  # noqa
    context: Context,
    backend: Backend,
    model: Model,
    dtype: Binary,
    value: bytes,
    *,
    select: dict = None,
):
    return base64.b64encode(value).decode('ascii')


@commands.prepare_dtype_for_response.register(Context, Backend, Model, Ref, dict)
def prepare_dtype_for_response(  # noqa
    context: Context,
    backend: Backend,
    model: Model,
    dtype: Ref,
    value: dict,
    *,
    select: dict = None,
):
    return value


@commands.prepare_dtype_for_response.register(Context, Backend, Model, Ref, str)
def prepare_dtype_for_response(  # noqa
    context: Context,
    backend: Backend,
    model: Model,
    dtype: Ref,
    value: str,
    *,
    select: dict = None,
):
    # FIXME: Backend should never return references as strings! References
    #        should always be dicts.
    return {'_id': value}


@commands.prepare_dtype_for_response.register(Context, Backend, Model, Object, dict)
def prepare_dtype_for_response(  # noqa
    context: Context,
    backend: Backend,
    model: Model,
    dtype: Object,
    value: dict,
    *,
    select: dict = None,
):
    _check_unknown_props(dtype, select, set(value)),
    return {
        prop.name: commands.prepare_dtype_for_response(
            context,
            backend,
            model,
            prop.dtype,
            val,
            select=sel,
        )
        for prop, val, sel in _select_props(
            value.keys(),
            dtype.properties,
            value,
            select,
        )
    }


@commands.prepare_dtype_for_response.register(Context, Backend, Model, Array, list)
def prepare_dtype_for_response(  # noqa
    context: Context,
    backend: Backend,
    model: Model,
    dtype: Array,
    value: list,
    *,
    select: dict = None,
):
    return [
        commands.prepare_dtype_for_response(
            context,
            backend,
            model,
            dtype.items.dtype,
            v,
            select=select,
        )
        for v in value
    ]


@commands.prepare_dtype_for_response.register(Context, Backend, Model, Array, type(None))
def prepare_dtype_for_response(  # noqa
    context: Context,
    backend: Backend,
    model: Model,
    dtype: Array,
    value: type(None),
    *,
    select: dict = None,
):
    return []


@commands.prepare_dtype_for_response.register(Context, Backend, Model, JSON, object)
def prepare_dtype_for_response(  # noqa
    context: Context,
    backend: Backend,
    model: Model,
    dtype: JSON,
    value: object,
    *,
    select: dict = None,
):
    return value


@commands.prepare_dtype_for_response.register(Context, Backend, Model, JSON, NotAvailable)
def prepare_dtype_for_response(  # noqa
    context: Context,
    backend: Backend,
    model: Model,
    dtype: JSON,
    value: NotAvailable,
    *,
    select: dict = None,
):
    return None


@commands.unload_backend.register()
def unload_backend(context: Context, backend: Backend):
    pass


@load_operator_value.register()
def load_operator_value(context: Context, backend: Backend, dtype: DataType, value: object, *, query_params: dict):
    operator = query_params['name']
    # XXX: That is probably not a very reliable way of getting original operator
    #      name. Maybe at least this should be documented some how.
    operator_name = query_params.get('origin', operator)
    if operator in ('startswith', 'contains') and not isinstance(dtype, (String, PrimaryKey)):
        raise exceptions.InvalidOperandValue(dtype, operator=operator_name)
    if operator in ('gt', 'ge', 'lt', 'le') and isinstance(dtype, String):
        raise exceptions.InvalidOperandValue(dtype, operator=operator_name)


@commands.make_json_serializable.register()
def make_json_serializable(model: Model, value: dict) -> dict:
    return commands.make_json_serializable[Object, dict](model, value)


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(dtype: DataType, value: object):  # noqa
    return value


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(dtype: DateTime, value: datetime.datetime):  # noqa
    return value.isoformat()


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(dtype: Date, value: datetime.date):  # noqa
    return value.isoformat()


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(dtype: Date, value: datetime.datetime):  # noqa
    return value.date().isoformat()


def _get_json_serializable_props(dtype: Object, value: dict):
    for k, v in value.items():
        if k in dtype.properties:
            yield dtype.properties[k], v
        else:
            Exception(f"Unknown {k} key in {dtype!r} object.")


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(dtype: Object, value: dict):  # noqa
    return {
        prop.name: commands.make_json_serializable(prop.dtype, v)
        for prop, v in _get_json_serializable_props(dtype, value)
    }


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(dtype: Array, value: list):  # noqa
    return [make_json_serializable(dtype.items.dtype, v) for v in value]


@commands.update.register()
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


@commands.patch.register()
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


@commands.delete.register()
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


@commands.cast_backend_to_python.register()
def cast_backend_to_python(  # noqa
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


@commands.cast_backend_to_python.register()
def cast_backend_to_python(  # noqa
    context: Context,
    prop: Property,
    backend: Backend,
    data: object,
) -> dict:
    return commands.cast_backend_to_python(context, prop.dtype, backend, data)


@commands.cast_backend_to_python.register()
def cast_backend_to_python(  # noqa
    context: Context,
    dtype: DataType,
    backend: Backend,
    data: object,
) -> dict:
    return data


@commands.cast_backend_to_python.register()
def cast_backend_to_python(  # noqa
    context: Context,
    dtype: Object,
    backend: Backend,
    data: dict,
) -> dict:
    return {
        k: commands.cast_backend_to_python(
            context,
            dtype.properties[k].dtype,
            backend,
            v,
        ) if k in dtype.properties else v
        for k, v in data.items()
    }


@commands.cast_backend_to_python.register()
def cast_backend_to_python(  # noqa
    context: Context,
    dtype: Array,
    backend: Backend,
    data: list,
) -> dict:
    return [
        commands.cast_backend_to_python(
            context,
            dtype.items.dtype,
            backend,
            v,
        )
        for v in data
    ]


@commands.cast_backend_to_python.register()
def cast_backend_to_python(  # noqa
    context: Context,
    dtype: Binary,
    backend: Backend,
    data: str,
) -> dict:
    return base64.b64decode(data)


def log_getall(
    context: Context,
    rows: Generator[dict, None, None],
    select: List[str] = None,
    hidden: List[str] = None,
):
    accesslog = context.get('accesslog')
    for chunk in chunks(rows, accesslog.buffer_size):
        resources = [
            {
                '_id': data.get('_id'),
                '_type': data.get('_type'),
                '_revision': data.get('_revision')
            } for data in chunk
        ]
        fields = get_accessed_fields(chunk[0], select=select, hidden=hidden)
        accesslog.log(resources=resources, fields=fields)
        yield from chunk


def log_getone(
    context: Context,
    result: dict,
    select: List[str] = None,
    hidden: List[str] = None,
):
    fields = get_accessed_fields(result, select=select, hidden=hidden)
    # XXX: select queries do not return metadata
    resource = {
        '_id': result.get('_id'),  # XXX: subresource queries do not return id
        '_type': result.get('_type'),
        '_revision': result.get('_revision'),
    }
    accesslog = context.get('accesslog')
    accesslog.log(resources=[resource], fields=fields)


def get_accessed_fields(data, select=None, hidden=None):
    # Gathers the fields that user is accessing with a query.
    # It's the meta fields (_id, _type, _revision) with the all other object
    # non-meta fields which are selected (if no select - then all fields)
    all_keys = list(recursive_keys(data, dot_notation=True))
    accessed_fields = []
    meta = ['_id', '_type', '_revision']
    for key in all_keys:
        if select:
            # if select filter is given - return meta fields + select
            if key in select or key in meta:
                accessed_fields.append(key)
                continue
        else:
            if key in meta:
                accessed_fields.append(key)
                continue
            elif hidden and key in hidden:
                continue
            else:
                # check if key is a meta field, we need to return only meta fields
                # described in `meta` variable.
                not_meta_attr = [not attrs.startswith('_') for attrs in key.split('.')]
                if all(not_meta_attr):
                    accessed_fields.append(key)
                    continue

    return accessed_fields
