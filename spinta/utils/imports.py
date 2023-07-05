import importlib
import inspect
from typing import Any
from typing import Type

from spinta.exceptions import PackageMissing, ModuleNotInGroup


def importstr(path):
    if ':' not in path:
        raise Exception(
            f"Can't import python path: {path!r}. Python path must be in "
            f"'dotted.path:Name' form."
        )
    module, obj = path.split(':', 1)
    module = importlib.import_module(module)
    obj = getattr(module, obj)
    return obj


def full_class_name(obj: Any) -> str:
    klass: Type
    if not inspect.isclass(obj):
        klass = type(obj)
    else:
        klass = obj
    return f'{klass.__module__}.{klass.__name__}'


def use(group_name, module_name, package=None):
    DEPENDENCIES = {
        'http': ['spinta[http]'],
        'log': ['spinta[log]'],
        'pii': ['spinta[pii]'],
        'cli': ['spinta[cli]'],
        'yaml': ['spinta[yaml]'],
        'sql': ['spinta[sql]'],
        'excel': ['spinta[exce]'],
        'postgres': ['spinta[postgres]'],
        'mongo': ['spinta[mongo]'],
        'datasets': ['spinta[datasets]'],
        'xml': ['spinta[xml]'],
        'html': ['spinta[html]'],
        'json': ['spinta[json]'],
        'ascii': ['spinta[ascii]'],
        'rdf': ['spinta[rdf]'],
        'geometry': ['spinta[geometry]'],
        'test': ['spinta[test]'],
        'docs': ['spinta[docs]']
    }

    PACKAGES = {
        'http': {
            'starlette': 'http',
            'gunicorn': 'http',
            'uvicorn': 'http',
            'authlib': 'http',
            'python-multipart': 'http',
            'httpx': 'http',
            'aiohttp': 'http',
            'aiofiles': 'http',
        },
        'log': {
            'psutil': 'log'
        },
        'pii': {
            'phonenumbers': 'pii'
        },
        'cli': {
            'click': 'cli',
            'typer': 'cli'
        },
        'yaml': {
            'ruamel-yaml': 'yaml'
        },
        'sql': {
            'sqlparse': 'sql'
        },
        'excel': {
            'xlrd': 'excel',
            'XlsxWriter': 'excel',
            'openpyxl': 'excel',
        },
        'postgres': {
            'alembic': 'postgres',
            'asyncpg': 'postgres',
            'psycopg2-binary': 'postgres',
            'sqlalchemy': 'postgres',
        },
        'mongo': {
            'pymongo': 'mongo',
        },
        'datasets': {
            'msgpack': 'datasets',
            'requests': 'datasets',
            'fsspec': 'datasets',
            'dask': 'datasets',
            'tqdm': 'datasets',
        },
        'xml': {
            'lxml': 'xml'
        },
        'html': {
            'jinja2': 'html'
        },
        'json': {
            'ujson': 'json'
        },
        'ascii': {
            'tabulate': 'ascii'
        },
        'rdf': {
            'lxml': 'rdf'
        },
        'geometry': {
            'GeoAlchemy2': 'geometry',
            'Shapely': 'geometry',
            'pyproj': 'geometry',
        },
        'test': {
            'pytz': 'test',
        },
        'docs': {
            'sphinx': 'docs',
            'sphinx-autobuild': 'docs',
            'sphinxcontrib-httpdomain': 'docs',
            'memory-profiler': 'docs',
            'mypy': 'docs',
            'cssselect': 'docs',
            'objprint': 'docs',
            'sphinx-rtd-theme': 'docs',
            'sqlalchemy-stubs': 'docs',
        }

    }

    needed_modules = PACKAGES[group_name]
    dependency = DEPENDENCIES[group_name]
    full_module_name = module_name
    if '.' in module_name:
        module_name = module_name.split('.')[0]
    if module_name in needed_modules:
        try:
            module = importlib.import_module(full_module_name, package=package)
        except Exception as e:
            raise PackageMissing(feature=group_name, dependency=dependency[0], module=module_name)
        return module
    else:
        raise ModuleNotInGroup(module=module_name, group=group_name)
