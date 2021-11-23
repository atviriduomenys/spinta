import importlib
import pathlib
from typing import Type
from typing import TypeVar

from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.core.config import configure_rc

from spinta.core.config import read_config
from spinta.utils.imports import importstr


ContextType = TypeVar('ContextType', bound=Context)


def create_context(
    name='spinta',
    rc=None,
    context: ContextType = None,
    args=None,
    envfile=None,
) -> ContextType:
    if rc is None:
        rc = read_config(args, envfile)

    load_commands(rc.get('commands', 'modules', cast=list))

    if context is None:
        Context_: Type[Context] = rc.get('components', 'core', 'context', cast=importstr, required=True)
        context = Context_(name)

    context.set('rc', rc)

    Config = rc.get('components', 'core', 'config', cast=importstr, required=True)
    context.set('config', Config())

    Store = rc.get('components', 'core', 'store', cast=importstr, required=True)
    context.set('store', Store())

    return context


def load_commands(modules):
    for module_path in modules:
        module = importlib.import_module(module_path)
        path = pathlib.Path(module.__file__).resolve()
        if path.name != '__init__.py':
            continue
        path = path.parent
        base = path.parents[module_path.count('.')]
        for path in path.glob('**/*.py'):
            if path.name == '__init__.py':
                module_path = path.parent.relative_to(base)
            else:
                module_path = path.relative_to(base).with_suffix('')
            module_path = '.'.join(module_path.parts)
            module = importlib.import_module(module_path)


def configure_context(context: Context, *args, **kwargs) -> Context:
    rc: RawConfig = context.get('rc')
    context = context.fork('configure')
    context.set('rc', configure_rc(rc, *args, **kwargs))
    return context
