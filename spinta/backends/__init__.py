import contextlib
import datetime

from spinta.types.type import Type, DateTime, Date, Object, Array
from spinta.components import Context, Model, Property, Action
from spinta.commands import error, prepare, dump, check
from spinta.common import NA
from spinta.types import dataset


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
def dump(context: Context, backend: Backend, type: Type, value: object):
    return value


@dump.register()
def dump(context: Context, backend: Backend, type: DateTime, value: datetime.datetime):
    return value.isoformat()


@dump.register()
def dump(context: Context, backend: Backend, type: Date, value: datetime.date):
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    else:
        return value.isoformat()


@dump.register()
def dump(context: Context, backend: Backend, type: Object, value: dict):
    return {
        prop.name: dump(context, backend, prop.type, value.get(prop.name))
        for prop in type.properties.values()
    }


@dump.register()
def dump(context: Context, backend: Backend, type: Array, value: list):
    return [dump(context, backend, type.items.type, v) for v in value]


@check.register()
def check(context: Context, model: Model, backend: Backend, data: dict):
    check_model_properties(context, model, backend, data)


@check.register()
def check(context: Context, type: Type, prop: Property, backend: Backend, value: object, *, data: dict, action: Action):
    check_type_value(type, value)


@check.register()
def check(context: Context, type: Type, prop: dataset.Property, backend: Backend, value: object, *, data: dict, action: Action):
    check_type_value(type, value)


def check_model_properties(context: Context, model: Model, backend, data: dict, action: Action):
    for name, prop in model.properties.items():
        # For datasets, property type is optional.
        # XXX: but I think, it should be mandatory.
        if prop.type is not None:
            check(context, prop.type, prop, backend, data.get(name, NA), data=data, action=action)


def check_type_value(type: Type, value: object):
    if type.required and (value is None or value is NA):
        raise Exception(f"{type.prop.name!r} is required for {type.prop.model.name!r}.")
