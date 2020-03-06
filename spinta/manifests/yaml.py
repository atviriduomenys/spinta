import logging
import pathlib

from ruamel.yaml import YAML
from ruamel.yaml.parser import ParserError
from ruamel.yaml.scanner import ScannerError
from ruamel.yaml.error import YAMLError

from spinta.components import Context, Manifest
from spinta.utils.path import is_ignored
from spinta.config import RawConfig
from spinta import exceptions
from spinta import commands

log = logging.getLogger(__name__)

yaml = YAML(typ='safe')


class YamlManifest(Manifest):
    pass


@commands.load.register()
def load(context: Context, manifest: YamlManifest, rc: RawConfig):
    config = context.get('config')
    ignore = rc.get('ignore', default=[], cast=list)

    # Add all supported node types.
    for name in config.components['nodes'].keys():
        manifest.objects[name] = {}

    manifest.parent = None
    manifest.endpoints = {}

    log.info('Loading manifest %r from %s.', manifest.name, manifest.path.resolve())

    manifest.path = rc.get('manifests', name, 'path', cast=pathlib.Path, required=True)

    for file in manifest.path.glob('**/*.yml'):
        if is_ignored(ignore, manifest.path, file):
            continue

        try:
            data = next(yaml.load_all(file.read_text()))
        except (ParserError, ScannerError, YAMLError) as e:
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
