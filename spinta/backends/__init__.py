import contextlib

from spinta.types import Type
from spinta.components import Context, Model
from spinta.commands import error


class Backend(Type):
    metadata = {
        'name': 'backend',
    }

    @contextlib.contextmanager
    def transaction(self):
        raise NotImplementedError


@error.register()
def error(exc: Exception, context: Context, backend: Backend, type: Type, **kwargs):
    error(exc, context, type)


@error.register()
def error(exc: Exception, context: Context, model: Model, backend: Backend, **kwargs):
    error(exc, context, model)
