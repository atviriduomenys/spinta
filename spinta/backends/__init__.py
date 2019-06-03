import contextlib

from spinta.types.type import Array, Object, Type
from spinta.components import Context, Model, Property
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
    # FIXME: replace `prop.parent` to `prop.model`, because `prop.parent` can
    #        point to a parent property, but here we need a model.
    if backend.name == prop.parent.backend.name:
        return prepare(context, backend, prop.type)
    else:
        # When property backend differs from model backend, we need to call
        # `prepare` with two backends in order to return correct field type for
        # the model.
        return prepare(context, backend, prop.type, prop.parent.backend)


@prepare.register()
def prepare(context: Context, backend: Backend, prop: Property, model_backend: Backend):
    pass


@error.register()
def error(exc: Exception, context: Context, backend: Backend, type: Type, **kwargs):
    error(exc, context, type)


@error.register()
def error(exc: Exception, context: Context, model: Model, backend: Backend, **kwargs):
    error(exc, context, model)
