import contextlib

from spinta.types.type import Array, Object, Type
from spinta.components import Context, Model
from spinta.commands import error, prepare


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
    array_item_type = type.items.type
    new_prepare_array = []
    for item in value:
        new_prepare_array.append(prepare(context, array_item_type, backend, item))
    return new_prepare_array


@prepare.register()
def prepare(context: Context, type: Object, backend: Backend, value: dict) -> dict:
    # prepare objects and it's properties for datastore
    new_loaded_obj = {}
    for k, v in type.properties.items():
        new_loaded_obj[k] = prepare(context, v.type, backend, value[k])
    return new_loaded_obj


@error.register()
def error(exc: Exception, context: Context, backend: Backend, type: Type, **kwargs):
    error(exc, context, type)


@error.register()
def error(exc: Exception, context: Context, model: Model, backend: Backend, **kwargs):
    error(exc, context, model)
