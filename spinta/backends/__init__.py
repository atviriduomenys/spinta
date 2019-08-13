import contextlib
import datetime
import uuid
import typing

from spinta.types.type import Type, DateTime, Date, Object, Array
from spinta.components import Context, Model, Property, Action, Node
from spinta.commands import load_operator_value, error, prepare, dump, check, getone, gen_object_id, is_object_id
from spinta.common import NA
from spinta.types import dataset
from spinta.types.type import String
from spinta.exceptions import ConflictError, DataError, NotFound
from spinta.utils.nestedstruct import build_show_tree


class Backend:
    metadata = {
        'name': 'backend',
    }

    @contextlib.contextmanager
    def transaction(self):
        raise NotImplementedError


@prepare.register()
def prepare(context: Context, type: Type, backend: Backend, value: object) -> object:
    # prepares value for data store
    # for simple types - loaded native values should work
    # otherwise - override for this command if necessary
    return value


@prepare.register()
def prepare(context: Context, type: Array, backend: Backend, value: list) -> list:
    # prepare array and it's items for datastore
    return [prepare(context, type.items.type, backend, v) for v in value]


@prepare.register()
def prepare(context: Context, type: Object, backend: Backend, value: dict) -> dict:
    # prepare objects and it's properties for datastore
    new_loaded_obj = {}
    for k, v in type.properties.items():
        # only load value keys which are available in schema
        # FIXME: If k is not in value, then value should be NA. Do not skip a value
        if k in value:
            new_loaded_obj[k] = prepare(context, v.type, backend, value[k])
    return new_loaded_obj


@prepare.register()
def prepare(context: Context, backend: Backend, prop: Property):
    return prepare(context, backend, prop.type)


@error.register()
def error(exc: Exception, context: Context, backend: Backend, type: Type, **kwargs):
    error(exc, context, type)


@error.register()
def error(exc: Exception, context: Context, model: Model, backend: Backend, **kwargs):
    error(exc, context, model)


@dump.register()
def dump(context: Context, backend: Backend, type: Type, value: object, *, show: dict = None):
    return value


@dump.register()
def dump(context: Context, backend: Backend, type: DateTime, value: datetime.datetime, *, show: dict = None):
    return value.isoformat()


@dump.register()
def dump(context: Context, backend: Backend, type: Date, value: datetime.date, *, show: dict = None):
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    else:
        return value.isoformat()


@dump.register()
def dump(context: Context, backend: Backend, type: Object, value: dict, *, show: dict = None):
    if show is None or show[type.prop.place]:
        show_ = show
    else:
        show_ = {
            prop.place: set()
            for prop in type.properties.values()
        }
    return {
        prop.name: dump(context, backend, prop.type, value.get(prop.name), show=show_)
        for prop in type.properties.values()
        if show_ is None or prop.place in show_
    }


@dump.register()
def dump(context: Context, backend: Backend, type: Array, value: list, *, show: dict = None):
    return [dump(context, backend, type.items.type, v, show=show) for v in value]


@dump.register()
def dump(context: Context, backend: Backend, type: Array, value: type(None), *, show: dict = None):
    return []


@check.register()
def check(context: Context, model: Model, backend: Backend, data: dict):
    check_model_properties(context, model, backend, data)


@check.register()
def check(context: Context, type: Type, prop: Property, backend: Backend, value: object, *, data: dict, action: Action):
    check_type_value(type, value)


@check.register()
def check(context: Context, type: Type, prop: dataset.Property, backend: Backend, value: object, *, data: dict, action: Action):
    check_type_value(type, value)


def check_model_properties(context: Context, model: Model, backend, data: dict, action: Action, id: str):
    if action in [Action.UPDATE, Action.DELETE] and 'revision' not in data:
        raise DataError(f"'revision' must be given on rewrite operation.")

    REWRITE = [Action.UPDATE, Action.PATCH, Action.DELETE]
    if action in REWRITE:
        # FIXME: try to put query somewhere higher in the request handling stack.
        # We do not want to hide expensive queries or call same thing multiple times.
        row = getone(context, model, backend, id_=id)

        for k in ['id', 'type', 'revision']:
            if k in data and row[k] != data[k]:
                raise ConflictError(' '.join((
                    f"Given {k!r} value must match database.",
                    f"Given value: {data[k]!r}, existing value: {row[k]!r}."
                )))

    for name, prop in model.properties.items():
        # For datasets, property type is optional.
        # XXX: but I think, it should be mandatory.
        if prop.type is not None:
            check(context, prop.type, prop, backend, data.get(name, NA), data=data, action=action)


def check_type_value(type: Type, value: object):
    if type.required and (value is None or value is NA):
        raise Exception(f"{type.prop.name!r} is required for {type.prop.model.name!r}.")


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
                for model in resource.objects.values():
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


@load_operator_value.register()
def load_operator_value(context: Context, backend: Backend, type_: Type, value: object, *, operator: str):
    if operator in ['startswith', 'contains'] and not isinstance(value, str):
        raise DataError(' '.join((
            f"Operator '{operator}' requires string.",
            f"Received value for '{type_.prop.place}' is of type '{type(value)}'."
        )))

    if operator in ['gt', 'gte', 'lt', 'lte'] and isinstance(type_, String):
        raise DataError(
            f"Operator {operator!r} received value for {type_.prop.place!r} of type {type_.name!r}."
        )


def _prepare_query_result(
    context: Context,
    action: Action,
    model: Node,
    backend: Backend,
    value: dict,
    show: typing.List[str],
):
    if action in (Action.GETALL, Action.SEARCH, Action.GETONE):
        value = {**value, 'type': model.get_type_value()}
        result = {}

        if show is not None:
            unknown_properties = set(show) - {
                name
                for name, prop in model.flatprops.items()
                if not prop.hidden
            }
            if unknown_properties:
                raise NotFound("Unknown properties for show: %s" % ', '.join(sorted(unknown_properties)))
            show = build_show_tree(show)

        for prop in model.properties.values():
            if prop.hidden:
                continue
            if show is None or prop.place in show:
                result[prop.name] = dump(context, backend, prop.type, value.get(prop.name), show=show)

        return result

    elif action in (Action.INSERT, Action.UPDATE):
        result = {}
        for prop in model.properties.values():
            if prop.hidden:
                continue
            result[prop.name] = dump(context, backend, prop.type, value.get(prop.name))
        result['type'] = model.get_type_value()
        return result

    elif action == Action.PATCH:
        result = {}
        for k, v in value.items():
            prop = model.properties[k]
            result[prop.name] = dump(context, backend, prop.type, v)
        result['type'] = model.get_type_value()
        return result

    elif action == Action.DELETE:
        return None

    else:
        raise Exception(f"Unknown action {action}.")
