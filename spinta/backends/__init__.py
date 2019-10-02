import contextlib
import datetime
import uuid
import typing

from spinta.types.datatype import DataType, DateTime, Date, Object, Array, String
from spinta.components import Context, Model, Property, Action, Node
from spinta.commands import load_operator_value, prepare, dump, check, getone, gen_object_id, is_object_id
from spinta.common import NA
from spinta.types import dataset
from spinta.exceptions import ConflictingValue, ItemDoesNotExist, NoItemRevision, NotFoundError
from spinta.utils.nestedstruct import build_show_tree
from spinta.utils.url import Operator
from spinta import commands
from spinta import exceptions


class Backend:
    metadata = {
        'name': 'backend',
    }

    def __repr__(self):
        return f'<{self.__class__.__module__}.{self.__class__.__name__}(name={self.name!r}) at 0x{id(self):02x}>'

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
def dump(context: Context, backend: Backend, dtype: DataType, value: object, *, show: dict = None):
    return value


@dump.register()
def dump(context: Context, backend: Backend, dtype: DateTime, value: datetime.datetime, *, show: dict = None):
    return value.isoformat()


@dump.register()
def dump(context: Context, backend: Backend, dtype: Date, value: datetime.date, *, show: dict = None):
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    else:
        return value.isoformat()


@dump.register()
def dump(context: Context, backend: Backend, dtype: Object, value: dict, *, show: dict = None):
    if show is None or show[dtype.prop.place]:
        show_ = show
    else:
        show_ = {
            prop.place: set()
            for prop in dtype.properties.values()
        }
    return {
        prop.name: dump(context, backend, prop.dtype, value.get(prop.name), show=show_)
        for prop in dtype.properties.values()
        if show_ is None or prop.place in show_
    }


@dump.register()
def dump(context: Context, backend: Backend, dtype: Array, value: list, *, show: dict = None):
    return [dump(context, backend, dtype.items.dtype, v, show=show) for v in value]


@dump.register()
def dump(context: Context, backend: Backend, dtype: Array, value: type(None), *, show: dict = None):
    return []


@check.register()
def check(context: Context, model: Model, backend: Backend, data: dict):
    check_model_properties(context, model, backend, data)


@check.register()
def check(context: Context, dtype: DataType, prop: Property, backend: Backend, value: object, *, data: dict, action: Action):
    check_type_value(dtype, value)


@check.register()
def check(context: Context, dtype: DataType, prop: dataset.Property, backend: Backend, value: object, *, data: dict, action: Action):
    check_type_value(dtype, value)


def check_model_properties(context: Context, model: Model, backend, data: dict, action: Action, id_: str):
    if action in [Action.UPDATE, Action.DELETE] and 'revision' not in data:
        raise NoItemRevision(model)

    REWRITE = [Action.UPDATE, Action.PATCH, Action.DELETE]
    if action in REWRITE:
        # FIXME: try to put query somewhere higher in the request handling stack.
        # We do not want to hide expensive queries or call same thing multiple times.
        try:
            row = getone(context, model, backend, id_=id_)
        except NotFoundError:
            raise ItemDoesNotExist(model, id=id_)

        for k in ['id', 'type', 'revision']:
            if k in data and row[k] != data[k]:
                raise ConflictingValue(model.properties[k], given=data[k], expected=row[k])

    for name, prop in model.properties.items():
        # For datasets, property type is optional.
        # XXX: but I think, it should be mandatory.
        if prop.dtype is not None:
            check(context, prop.dtype, prop, backend, data.get(name, NA), data=data, action=action)


def check_type_value(dtype: DataType, value: object):
    if dtype.required and (value is None or value is NA):
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
    show: typing.List[str] = None,
) -> dict:
    return _prepare_query_result(context, action, model, backend, value, show)


@prepare.register()
def prepare(
    context: Context,
    action: Action,
    model: dataset.Model,
    backend: Backend,
    value: dict,
    *,
    show: typing.List[str] = None,
) -> dict:
    return _prepare_query_result(context, action, model, backend, value, show)


@commands.unload_backend.register()
def unload_backend(context: Context, backend: Backend):
    pass


@load_operator_value.register()
def load_operator_value(context: Context, backend: Backend, dtype: DataType, value: object, *, query_params: dict):
    operator = query_params['operator']
    operator_name = query_params['name']
    if operator in (Operator.STARTSWITH, Operator.CONTAINS) and not isinstance(dtype, String):
        raise exceptions.InvalidOperandValue(dtype, operator=operator_name)
    if operator in [Operator.GT, Operator.GTE, Operator.LT, Operator.LTE] and isinstance(dtype, String):
        raise exceptions.InvalidOperandValue(dtype, operator=operator_name)


def _prepare_query_result(
    context: Context,
    action: Action,
    model: Node,
    backend: Backend,
    value: dict,
    show: typing.List[str],
):
    if action in (Action.GETALL, Action.SEARCH, Action.GETONE):
        config = context.get('config')
        value = {**value, 'type': model.model_type()}

        if show is not None:
            unknown_properties = set(show) - {
                name
                for name, prop in model.flatprops.items()
                if not prop.hidden
            }
            if unknown_properties:
                raise exceptions.MultipleErrors(
                    exceptions.FieldNotInResource(model, property=prop)
                    for prop in sorted(unknown_properties)
                )

            if config.always_show_id and 'id' not in show:
                show = ['id'] + show

            show = build_show_tree(show)

        result = {}
        for prop in model.properties.values():
            if prop.hidden:
                continue
            if show is None or prop.place in show:
                result[prop.name] = dump(context, backend, prop.dtype, value.get(prop.name), show=show)

        return result

    elif action in (Action.INSERT, Action.UPDATE):
        result = {}
        for prop in model.properties.values():
            if prop.hidden:
                continue
            result[prop.name] = dump(context, backend, prop.dtype, value.get(prop.name))
        result['type'] = model.model_type()
        return result

    elif action == Action.PATCH:
        result = {}
        for k, v in value.items():
            prop = model.properties[k]
            result[prop.name] = dump(context, backend, prop.dtype, v)
        result['type'] = model.model_type()
        return result

    elif action == Action.DELETE:
        return None

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
