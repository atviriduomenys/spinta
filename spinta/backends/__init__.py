from typing import AsyncIterator, Union

import contextlib
import datetime
import uuid
import typing

from spinta.types.datatype import DataType, DateTime, Date, Object, Array, String
from spinta.components import Context, Model, Property, Action, Node, DataItem
from spinta.commands import load_operator_value, prepare, dump, gen_object_id, is_object_id
from spinta.common import NA
from spinta.types import dataset
from spinta.exceptions import ConflictingValue, NoItemRevision
from spinta.utils.nestedstruct import build_select_tree
from spinta import commands
from spinta import exceptions


class Backend:
    metadata = {
        'name': 'backend',
    }

    def __repr__(self):
        return (
            f'<{self.__class__.__module__}.{self.__class__.__name__}'
            f'(name={self.name!r}) at 0x{id(self):02x}>'
        )

    @contextlib.contextmanager
    def transaction(self):
        raise NotImplementedError


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


@dump.register()
def dump(context: Context, backend: Backend, dtype: DataType, value: object, *, select: dict = None):
    return value


@dump.register()
def dump(context: Context, backend: Backend, dtype: DateTime, value: datetime.datetime, *, select: dict = None):
    return value.isoformat()


@dump.register()
def dump(context: Context, backend: Backend, dtype: Date, value: datetime.date, *, select: dict = None):
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    else:
        return value.isoformat()


@dump.register()
def dump(context: Context, backend: Backend, dtype: Object, value: dict, *, select: dict = None):
    if select is None or '*' in select or select[dtype.prop.place]:
        select_ = select
    else:
        select_ = {
            prop.place: set()
            for prop in dtype.properties.values()
        }
    return {
        prop.name: dump(context, backend, prop.dtype, value.get(prop.name), select=select_)
        for prop in dtype.properties.values()
        if select_ is None or '*' in select_ or prop.place in select_
    }


@dump.register()
def dump(context: Context, backend: Backend, dtype: Array, value: list, *, select: dict = None):
    return [dump(context, backend, dtype.items.dtype, v, select=select) for v in value]


@dump.register()
def dump(context: Context, backend: Backend, dtype: Array, value: type(None), *, select: dict = None):
    return []


@commands.simple_data_check.register()
def simple_data_check(
    context: Context,
    data: DataItem,
    model: (Model, dataset.Model),
    backend: Backend,
) -> None:
    simple_model_properties_check(context, data, model, backend)


@commands.simple_data_check.register()  # noqa
def simple_data_check(
    context: Context,
    data: DataItem,
    prop: (Property, dataset.Property),
    backend: Backend,
) -> None:
    simple_data_check(
        context,
        data,
        prop.dtype,
        prop,
        backend,
        data.given,
    )


def simple_model_properties_check(
    context: Context,
    data: DataItem,
    model: Model,
    backend: Backend,
) -> None:
    for name, prop in model.properties.items():
        # For datasets, property type is optional.
        # XXX: but I think, it should be mandatory.
        if prop.dtype is not None:
            if prop.backend.name == backend.name:
                backend_ = backend
            else:
                backend_ = prop.backend
            simple_data_check(
                context,
                data,
                prop.dtype,
                prop,
                backend_,
                data.given.get(name, NA),
            )


@commands.simple_data_check.register()  # noqa
def simple_data_check(
    context: Context,
    data: DataItem,
    dtype: DataType,
    prop: Property,
    backend: Backend,
    value: object,
) -> None:
    check_type_value(dtype, value)


@commands.simple_data_check.register()  # noqa
def simple_data_check(
    context: Context,
    data: DataItem,
    dtype: DataType,
    prop: dataset.Property,
    backend: Backend,
    value: object,
) -> None:
    check_type_value(dtype, value)


@commands.complex_data_check.register()
def complex_data_check(
    context: Context,
    data: DataItem,
    model: Model,
    backend: Backend,
) -> None:
    _complex_data_check(context, data, model, backend)


@commands.complex_data_check.register()  # noqa
def complex_data_check(
    context: Context,
    data: DataItem,
    model: dataset.Model,
    backend: Backend,
) -> None:
    _complex_data_check(context, data, model, backend)


def _complex_data_check(
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
            if prop.backend.name == backend.name:
                backend_ = backend
            else:
                backend_ = prop.backend
            complex_data_check(
                context,
                data,
                prop.dtype,
                prop,
                backend_,
                data.given.get(name, NA),
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
    pass


@complex_data_check.register()
def complex_data_check(
    context: Context,
    data: DataItem,
    dtype: DataType,
    prop: dataset.Property,
    backend: Backend,
    value: object,
):
    pass


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
    #      information about model, resource and dataset, and then call
    #      is_object_id directly with backend and model. That would be nice.
    store = context.get('store')
    candidates = set()
    for manifest in store.manifests.values():
        for model in manifest.objects.get('model', {}).values():
            candidates.add((model.backend.__class__, model.__class__))
        for dataset_ in manifest.objects.get('dataset', {}).values():
            for resource in dataset_.resources.values():
                for model in resource.models():
                    candidates.add((model.backend.__class__, model.__class__))

    # Currently all models use UUID, but dataset models use id generated from
    # other properties that form an unique id.
    for Backend, Model_ in candidates:
        backend = Backend()
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


@prepare.register()
def prepare(
    context: Context,
    action: Action,
    model: Model,
    backend: Backend,
    value: dict,
    *,
    select: typing.List[str] = None,
) -> dict:
    return _prepare_query_result(context, action, model, backend, value, select)


@prepare.register()
def prepare(
    context: Context,
    action: Action,
    model: dataset.Model,
    backend: Backend,
    value: dict,
    *,
    select: typing.List[str] = None,
) -> dict:
    return _prepare_query_result(context, action, model, backend, value, select)


@commands.unload_backend.register()
def unload_backend(context: Context, backend: Backend):
    pass


@load_operator_value.register()
def load_operator_value(context: Context, backend: Backend, dtype: DataType, value: object, *, query_params: dict):
    operator = query_params['name']
    # XXX: That is probably not a very reliable way of getting original operator
    #      name. Maybe at least this should be documented some how.
    operator_name = query_params.get('origin', operator)
    if operator in ('startswith', 'contains') and not isinstance(dtype, String):
        raise exceptions.InvalidOperandValue(dtype, operator=operator_name)
    if operator in ('gt', 'ge', 'lt', 'le') and isinstance(dtype, String):
        raise exceptions.InvalidOperandValue(dtype, operator=operator_name)


def _prepare_query_result(
    context: Context,
    action: Action,
    model: Node,
    backend: Backend,
    value: dict,
    select: typing.List[str],
):
    if action in (Action.GETALL, Action.SEARCH, Action.GETONE):
        config = context.get('config')
        value = {**value, '_type': model.model_type()}

        if select is not None:
            unknown_properties = set(select) - {
                name
                for name, prop in model.flatprops.items()
                if not prop.hidden
            } - {'*'}
            if unknown_properties:
                raise exceptions.MultipleErrors(
                    exceptions.FieldNotInResource(model, property=prop)
                    for prop in sorted(unknown_properties)
                )

            if config.always_show_id and '_id' not in select:
                select = ['_id'] + select

            select = build_select_tree(select)

        elif action in (Action.GETALL, Action.SEARCH) and config.always_show_id:
            select = ['_id']

        result = {}
        for prop in model.properties.values():
            if prop.hidden or (prop.name.startswith('_') and prop.name not in ('_id', '_revision', '_type')):
                continue
            if select is None or '*' in select or prop.place in select:
                result[prop.name] = dump(context, backend, prop.dtype, value.get(prop.name), select=select)

        return result

    elif action in (Action.INSERT, Action.UPDATE, Action.UPSERT):
        result = {}
        for prop in model.properties.values():
            if prop.hidden:
                continue
            result[prop.name] = dump(context, backend, prop.dtype, value.get(prop.name))
        result['_type'] = model.model_type()
        result['_id'] = value.get('_id')
        result['_revision'] = value.get('_revision')
        return result

    elif action == Action.PATCH:
        result = {}
        for k, v in value.items():
            prop = model.properties[k]
            result[prop.name] = dump(context, backend, prop.dtype, v)
        result['_type'] = model.model_type()
        result['_id'] = value.get('_id')
        result['_revision'] = value.get('_revision')
        return result

    elif action == Action.DELETE:
        result = {}
        result['_type'] = model.model_type()
        result['_id'] = value.get('_id')
        result['_revision'] = value.get('_revision')
        return result

    else:
        raise Exception(f"Unknown action {action}.")


@commands.make_json_serializable.register()
def make_json_serializable(model: Model, value: dict) -> dict:
    return commands.make_json_serializable[Object, dict](model, value)


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(dtype: DataType, value: object):
    return value


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(dtype: DateTime, value: datetime.datetime):
    return value.isoformat()


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(dtype: Date, value: datetime.date):
    return value.isoformat()


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(dtype: Date, value: datetime.datetime):
    return value.date().isoformat()


def _get_json_serializable_props(dtype: Object, value: dict):
    for k, v in value.items():
        if k in dtype.properties:
            yield dtype.properties[k], v
        else:
            Exception(f"Unknown {k} key in {dtype!r} object.")


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(dtype: Object, value: dict):
    return {
        prop.name: commands.make_json_serializable(prop.dtype, v)
        for prop, v in _get_json_serializable_props(dtype, value)
    }


@commands.make_json_serializable.register()  # noqa
def make_json_serializable(dtype: Array, value: list):
    return [make_json_serializable(dtype.items.dtype, v) for v in value]


@commands.update.register()
def update(
    context: Context,
    prop: (Property, dataset.Property),
    backend: Backend,
    *,
    dstream: AsyncIterator[DataItem],
    stop_on_error: bool = True,
):
    dstream = _prepare_property_for_update(prop, dstream)
    return commands.update(
        context, prop.model, prop.model.backend,
        dstream=dstream,
        stop_on_error=stop_on_error,
    )


async def _prepare_property_for_update(
    prop: Union[Property, dataset.Property],
    dstream: AsyncIterator[DataItem],
) -> AsyncIterator[DataItem]:
    async for data in dstream:
        if data.patch:
            data.patch = {
                **{k: v for k, v in data.patch.items() if k.startswith('_')},
                prop.name: {
                    k: v for k, v in data.patch.items() if not k.startswith('_')
                }
            }
        yield data
