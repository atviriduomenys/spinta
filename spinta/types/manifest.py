from typing import Dict

import logging

from ruamel.yaml import YAML
from ruamel.yaml.parser import ParserError
from ruamel.yaml.scanner import ScannerError

from spinta.utils.path import is_ignored
from spinta.commands import load, prepare, check
from spinta.components import Context, Manifest, Node
from spinta.config import RawConfig
from spinta import commands
from spinta import exceptions

yaml = YAML(typ='safe')

log = logging.getLogger(__name__)


@load.register()
def load(context: Context, manifest: Manifest, c: RawConfig):
    config = context.get('config')
    ignore = c.get('ignore', default=[], cast=list)

    # Add all supported node types.
    # XXX: Not sure if this is a good idea to hardcode 'ns' here, will see.
    manifest.objects['ns'] = {}
    for name in config.components['nodes'].keys():
        manifest.objects[name] = {}

    manifest.parent = None
    manifest.endpoints = {}

    log.info('Loading manifest %r: %s', manifest.name, manifest.path.resolve())

    for file in manifest.path.glob('**/*.yml'):
        if is_ignored(ignore, manifest.path, file):
            continue

        try:
            data = yaml.load(file.read_text())
        except (ParserError, ScannerError) as e:
            raise exceptions.InvalidManifestFile(
                manifest=name,
                filename=file,
                error=str(e),
            )
        if not isinstance(data, dict):
            raise exceptions.InvalidManifestFile(
                manifest=name,
                filename=file,
                error=f"Expected dict got {data.__class__.__name__}.",
            )

        if 'type' not in data:
            raise exceptions.InvalidManifestFile(
                manifest=name,
                filename=file,
                error=f"Required parameter 'type' is not defined.",
            )

        if data['type'] not in manifest.objects:
            raise exceptions.InvalidManifestFile(
                manifest=name,
                filename=file,
                error=f"Unknown type {data['type']!r}.",
            )

        node = config.components['nodes'][data['type']]()
        data = {
            'path': file,
            'parent': manifest,
            'backend': manifest.backend,
            **data,
        }
        load(context, node, data, manifest)

        if node.name in manifest.objects[node.type]:
            raise exceptions.InvalidManifestFile(
                manifest=name,
                filename=file,
                error=f"Node {node.type} with name {node.name} already defined in {manifest.objects[node.type].path}.",
            )

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

    # Check dataset names in config.
    config = context.get('config')
    known_datasets = set(manifest.objects.get('dataset', {}).keys())
    datasets_in_config = set(config.raw.keys('datasets', manifest.name))
    unknown_datasets = datasets_in_config - known_datasets
    if unknown_datasets:
        raise Exception("Unknown datasets in configuration parameter 'datasets': %s." % (
            ', '.join([repr(x) for x in sorted(unknown_datasets)])
        ))

    # Check endpoints.
    names = {model.name for model in manifest.find_all_models()}
    for model in manifest.find_all_models():
        if model.endpoint == model.name or model.endpoint in names:
            raise Exception(f"Endpoint name can't overshadow existing model names and {model.endpoint!r} is already a model name.")


@commands.get_error_context.register()
def get_error_context(manifest: Manifest, *, prefix='this') -> Dict[str, str]:
    return {
        'schema': f'{prefix}.path.__str__()',
        'manifest': f'{prefix}.name'
    }


@commands.get_error_context.register()  # noqa
def get_error_context(node: Node, *, prefix='this') -> Dict[str, str]:
    context = {
        'schema': f'{prefix}.path.__str__()',
    }
    depth = 0
    while node:
        name = prefix + ('.' if depth else '') + '.'.join(['parent'] * depth)
        if isinstance(node, Manifest):
            context['manifest'] = f'{name}.name'
            break
        elif node.type:
            context[node.type] = f'{name}.name'
        depth += 1
        node = node.parent
    return context
