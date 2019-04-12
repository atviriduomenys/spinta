from ruamel.yaml import YAML
from ruamel.yaml.parser import ParserError
from ruamel.yaml.scanner import ScannerError

from spinta.utils.path import is_ignored
from spinta.commands import load, prepare, migrate, check
from spinta.types.project import Project
from spinta.types.dataset import Dataset
from spinta.types.owner import Owner
from spinta.components import Context, Manifest, Model

yaml = YAML(typ='safe')


class Path:
    metadata = {
        'name': 'path',
    }


@load.register()
def load(context: Context, manifest: Manifest, manifest_conf: dict):
    store = context.get('store')
    config = context.get('config')
    manifest.name = manifest_conf['name']
    manifest.path = manifest_conf['path']
    manifest.backend = store.backends[manifest_conf['backend']]
    for file in manifest.path.glob('**/*.yml'):
        if is_ignored(config.ignore, manifest_conf['path'], file):
            continue

        try:
            data = yaml.load(file.read_text())
        except (ParserError, ScannerError) as e:
            context.error(f"{file}: {e}.")
        if not isinstance(data, dict):
            context.error(f"{file}: expected dict got {data.__class__.__name__}.")

        object_types = {
            'model': Model,
            'project': Project,
            'dataset': Dataset,
            'owner': Owner,
        }

        obj = object_types[data['type']]()
        data = {
            'path': file,
            'parent': None,
            'backend': 'default',
            **data,
        }
        load(context, obj, data, manifest)

        if obj.type not in manifest.objects:
            manifest.objects[obj.type] = {}

        if obj.name in manifest.objects[obj.type]:
            context.error(f"Object {obj.type} with name {obj.name} already exist.")

        manifest.objects[obj.type][obj.name] = obj


@prepare.register()
def prepare(context: Context, manifest: Manifest):
    store = context.get('store')
    for backend in store.backends.values():
        prepare(context, backend, manifest)


@check.register()
def check(context: Context, manifest: Manifest):
    for objects in manifest.objects.values():
        for obj in objects.values():
            check(context, obj)
