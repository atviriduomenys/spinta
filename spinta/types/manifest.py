from ruamel.yaml import YAML
from ruamel.yaml.parser import ParserError
from ruamel.yaml.scanner import ScannerError

from spinta.utils.path import is_ignored
from spinta.commands import load, prepare, check
from spinta.components import Context, Manifest
from spinta.config import Config

yaml = YAML(typ='safe')


@load.register()
def load(context: Context, manifest: Manifest, c: Config):
    config = context.get('config')
    ignore = c.get('ignore', default=[], cast=list)

    for file in manifest.path.glob('**/*.yml'):
        if is_ignored(ignore, manifest.path, file):
            continue

        try:
            data = yaml.load(file.read_text())
        except (ParserError, ScannerError) as e:
            context.error(f"{file}: {e}.")
        if not isinstance(data, dict):
            context.error(f"{file}: expected dict got {data.__class__.__name__}.")

        if 'type' not in data:
            raise Exception(f"'type' is not defined in {file}.")

        if data['type'] not in config.components['nodes']:
            raise Exception(f"Unknown type {data['type']!r} in {file}.")

        node = config.components['nodes'][data['type']]()
        data = {
            'path': file,
            'parent': None,
            'backend': 'default',
            **data,
        }
        load(context, node, data, manifest)

        if node.type not in manifest.objects:
            manifest.objects[node.type] = {}

        if node.name in manifest.objects[node.type]:
            context.error(f"Object {node.type} with name {node.name} already exist.")

        manifest.objects[node.type][node.name] = node


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
