import inspect
import importlib
import pathlib
import time
import types

from spinta import commands
from spinta.commands import load, wait, init, migrate, check, push
from spinta.components import Context, Store
from spinta.urlparams import get_model_by_name
from spinta.nodes import load_manifest, get_internal_manifest, get_node
from spinta.core.config import RawConfig
from spinta.nodes import create_component




@check.register()
def check(context: Context, store: Store):
    check(context, store.manifest)


@init.register()
def init(context: Context, store: Store):
    init(context, store.manifest)


@commands.freeze.register()
def freeze(context: Context, store: Store) -> bool:
    commands.freeze(context, store.manifest)


@commands.bootstrap.register()
def bootstrap(context: Context, store: Store) -> bool:
    commands.bootstrap(context, store.manifest)


@commands.sync.register()
def sync(context: Context, store: Store):
    commands.sync(context, store.manifest)


@migrate.register()
def migrate(context: Context, store: Store):
    commands.migrate(context, store.manifest)


@push.register()
def push(context: Context, store: Store, stream: types.GeneratorType):
    manifest = store.manifest
    for data in stream:
        data = dict(data)
        model_name = data.pop('type', None)
        assert model_name is not None, data
        model = get_model_by_name(context, manifest, model_name)
        if 'id' in data:
            id_ = commands.upsert(context, model, model.backend, key=['id'], data=data)
        else:
            id_ = commands.insert(context, model, model.backend, data=data)
        if id_ is not None:
            yield id_


@push.register()
def push(context: Context, store: Store, stream: list):
    yield from push(context, store, (x for x in stream))


def find_subclasses(Class, modules):
    for module_path in modules:
        module = importlib.import_module(module_path)
        path = pathlib.Path(module.__file__).parent
        base = path.parents[module_path.count('.')]
        for path in path.glob('**/*.py'):
            if path.name == '__init__.py':
                module_path = path.parent.relative_to(base)
            else:
                module_path = path.relative_to(base).with_suffix('')
            module_path = '.'.join(module_path.parts)
            module = importlib.import_module(module_path)
            for obj_name in dir(module):
                obj = getattr(module, obj_name)
                if inspect.isclass(obj) and issubclass(obj, Class) and obj is not Class and obj.__module__ == module_path:
                    yield obj
