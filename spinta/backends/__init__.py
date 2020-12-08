from typing import AsyncIterator, Union, List, Dict, Optional, Iterable, Generator

import base64
import datetime
import decimal
import pathlib
import uuid
import typing

from spinta import spyna
from spinta import commands
from spinta import exceptions
from spinta.auth import authorized
from spinta.commands import load_operator_value, prepare, gen_object_id, is_object_id
from spinta.components import Context, Namespace, Model, Property, Action, Node, DataItem
from spinta.exceptions import ConflictingValue, NoItemRevision
from spinta.types.datatype import DataType, DateTime, Date, Object, Array, String, File, PrimaryKey, Binary, Ref, JSON, Number
from spinta.utils.itertools import chunks
from spinta.utils.schema import NotAvailable, NA
from spinta.utils.data import take
from spinta.backends.components import Backend

SelectTree = Optional[Dict[str, dict]]


@prepare.register(Context, DataType, Backend, object)
def prepare(context: Context, dtype: DataType, backend: Backend, value: object) -> object:
    # prepares value for data store
    # for simple types - loaded native values should work
    # otherwise - override for this command if necessary
    return value


@prepare.register(Context, Array, Backend, list)
def prepare(context: Context, dtype: Array, backend: Backend, value: list) -> list:
    # prepare array and it's items for datastore
    return [prepare(context, dtype.items.dtype, backend, v) for v in value]


@prepare.register(Context, Object, Backend, dict)
def prepare(context: Context, dtype: Object, backend: Backend, value: dict) -> dict:
    # prepare objects and it's properties for datastore
    new_loaded_obj = {}
    for k, prop in dtype.properties.items():
        # only load value keys which are available in schema
        # FIXME: If k is not in value, then value should be NA. Do not skip a value
        if k in value:
            new_loaded_obj[k] = prepare(context, prop.dtype, backend, value[k])
    return new_loaded_obj


@prepare.register(Context, Backend, Property)
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

    # Action.DELETE is ignore for qvarn compatibility reasons.
    # XXX: make `spinta` check for revision on Action.DELETE,
    #      implementers can override this command as they please.
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


@complex_data_check.register(Context, DataItem, DataType, Property, Backend, object)
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


@gen_object_id.register(Context, Backend, Node)
def gen_object_id(context: Context, backend: Backend, model: Node):
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


@commands.prepare_data_for_response.register(Context, Action, Model, Backend, dict)
def prepare_data_for_response(
    context: Context,
    action: Action,
    model: Model,
    backend: Backend,
    value: dict,
    *,
    select: typing.List[str] = None,
) -> dict:
    select = _apply_always_show_id(context, action, select)
    if select is None and action in (Action.GETALL, Action.SEARCH):
        # If select is not given, select everything.
        select = {'*': {}}
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
            context,
            action,
            model,
            value,
            select,
            ['_type', '_id', '_revision'],
        )
    }


@commands.prepare_data_for_response.register(Context, Action, Object, Backend, dict)
def prepare_data_for_response(
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
def prepare_data_for_response(
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

    for key, val, sel in _select_props(prop, reserved, None, value, select):
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
    context: Context,
    action: Action,
    model: Model,
    value: dict,
    select: SelectTree,
    reserved: List[str],
):
    _check_unknown_props(model, select, set(reserved) | set(take(model.properties)))

    if select is None:
        keys = value.keys()
    elif '*' in select:
        keys = [
            p.name
            for p in model.properties.values() if (
                not p.name.startswith('_') and
                not p.hidden and
                authorized(context, p, action)
            )
        ]
    else:
        keys = [k for k in select if not k.startswith('_')]

    yield from _select_props(
        model,
        reserved,
        model.properties,
        value,
        select,
    )
    yield from _select_props(
        model,
        keys,
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
        prop,
        reserved,
        prop.model.properties,
        value,
        select,
    )
    if prop.name in value:
        yield from _select_props(
            prop,
            value[prop.name].keys(),
            prop.dtype.properties,
            value[prop.name],
            select,
        )


def _select_props(
    node: Union[Namespace, Model, Property],
    keys: Iterable[str],
    props: Optional[Dict[str, Property]],
    value: dict,
    select: SelectTree,
    *,
    reserved: bool = True,
):
    for k in keys:

        if select is None and k not in value:
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

        if sel is not None and sel == {}:
            sel = {'*': {}}

        if props is None:
            prop = k
        else:
            if k not in props:
                # FIXME: We should check select list at the very beginning of
                #        request, not when returning results.
                raise exceptions.FieldNotInResource(node, property=k)
            prop = props[k]
            if prop.hidden:
                continue

        if k in value:
            val = value[k]
        else:
            val = None

        yield prop, val, sel


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
        if isinstance(v, dict):
            v = spyna.unparse(v)
        names = v.split('.')
        vref = res
        for name in names:
            if name not in vref:
                vref[name] = {}
            vref = vref[name]

    return res


@commands.prepare_dtype_for_response.register(Context, Backend, Model, DataType, object)
def prepare_dtype_for_response(
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
def prepare_dtype_for_response(
    context: Context,
    backend: Backend,
    model: Model,
    dtype: DataType,
    value: NotAvailable,
    *,
    select: dict = None,
):
    return dtype.default


@commands.prepare_dtype_for_response.register(Context, Backend, Model, File, NotAvailable)
def prepare_dtype_for_response(
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
def prepare_dtype_for_response(
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
            dtype.prop,
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
def prepare_dtype_for_response(
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
def prepare_dtype_for_response(
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
def prepare_dtype_for_response(
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
def prepare_dtype_for_response(
    context: Context,
    backend: Backend,
    model: Model,
    dtype: Ref,
    value: dict,
    *,
    select: dict = None,
):
    if select is None and value == {'_id': None}:
        return None
    return {
        prop.name: commands.prepare_dtype_for_response(
            context,
            backend,
            prop.model,
            prop.dtype,
            val,
            select=sel,
        )
        for prop, val, sel in _select_ref_props(
            dtype,
            value,
            select,
            ['_id'],
        )
    }


def _select_ref_props(
    dtype: Ref,
    value: dict,
    select: SelectTree,
    reserved: List[str],
):
    yield from _select_props(
        dtype.prop,
        reserved,
        dtype.model.properties,
        value,
        select,
    )
    yield from _select_props(
        dtype.model,
        value.keys(),
        dtype.model.properties,
        value,
        select,
    )


@commands.prepare_dtype_for_response.register(Context, Backend, Model, Ref, str)
def prepare_dtype_for_response(
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
def prepare_dtype_for_response(
    context: Context,
    backend: Backend,
    model: Model,
    dtype: Object,
    value: dict,
    *,
    select: dict = None,
):
    _check_unknown_props(dtype, select, set(take(dtype.properties)))

    if select is None:
        keys = value.keys()
    elif '*' in select:
        keys = take(dtype.properties).keys()
    else:
        keys = [k for k in select if not k.startswith('_')]

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
            dtype.prop,
            keys,
            dtype.properties,
            value,
            select,
        )
    }


@commands.prepare_dtype_for_response.register(Context, Backend, Model, Array, list)
def prepare_dtype_for_response(
    context: Context,
    backend: Backend,
    model: Model,
    dtype: Array,
    value: list,
    *,
    select: dict = None,
) -> list:
    return _prepare_array_for_response(
        context,
        backend,
        model,
        dtype,
        value,
        select,
    )


@commands.prepare_dtype_for_response.register(Context, Backend, Model, Array, tuple)
def prepare_dtype_for_response(
    context: Context,
    backend: Backend,
    model: Model,
    dtype: Array,
    value: tuple,
    *,
    select: dict = None,
) -> list:
    return _prepare_array_for_response(
        context,
        backend,
        model,
        dtype,
        value,
        select,
    )


def _prepare_array_for_response(
    context: Context,
    backend: Backend,
    model: Model,
    dtype: Array,
    value: Iterable,
    select: dict = None,
) -> list:
    if dtype.items:
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
    else:
        return [v for v in value]


@commands.prepare_dtype_for_response.register(Context, Backend, Model, Array, type(None))
def prepare_dtype_for_response(
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
def prepare_dtype_for_response(
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
def prepare_dtype_for_response(
    context: Context,
    backend: Backend,
    model: Model,
    dtype: JSON,
    value: NotAvailable,
    *,
    select: dict = None,
):
    return None


@commands.prepare_dtype_for_response.register(Context, Backend, Model, Number, decimal.Decimal)
def prepare_dtype_for_response(
    context: Context,
    backend: Backend,
    model: Model,
    dtype: Number,
    value: decimal.Decimal,
    *,
    select: dict = None,
):
    return float(value)


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
    data: object,
) -> dict:
    return data


@commands.cast_backend_to_python.register(Context, Object, Backend, dict)
def cast_backend_to_python(
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


@commands.cast_backend_to_python.register(Context, Array, Backend, list)
def cast_backend_to_python(
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


@commands.cast_backend_to_python.register(Context, Binary, Backend, str)
def cast_backend_to_python(
    context: Context,
    dtype: Binary,
    backend: Backend,
    data: str,
) -> dict:
    return base64.b64decode(data)


def log_getall(
    context: Context,
    rows: Generator[dict, None, None],
    select: List[dict] = None,
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
        fields = _get_selected_fields(select)
        accesslog.log(resources=resources, fields=fields)
        yield from chunk


def log_getone(
    context: Context,
    result: dict,
    select: List[str] = None,
    hidden: List[str] = None,
):
    fields = _get_selected_fields(select)
    # XXX: select queries do not return metadata
    resource = {
        '_id': result.get('_id'),  # XXX: subresource queries do not return id
        '_type': result.get('_type'),
        '_revision': result.get('_revision'),
    }
    accesslog = context.get('accesslog')
    accesslog.log(resources=[resource], fields=fields)


def _get_selected_fields(select: Optional[List[dict]]) -> List[str]:
    if select:
        return sorted([_get_selected_field(x) for x in select])
    else:
        return []


def _get_selected_field(ast: dict):
    if ast['name'] == 'bind':
        return ast['args'][0]
    if ast['name'] == 'getattr':
        return '.'.join([
            _get_selected_field(ast['args'][0]),
            _get_selected_field(ast['args'][1]),
        ])
