from typing import Iterable

import logging
import pathlib

import jsonpatch

from ruamel.yaml import YAML
from ruamel.yaml.parser import ParserError
from ruamel.yaml.scanner import ScannerError
from ruamel.yaml.error import YAMLError

from spinta.components import Context, Config, Manifest
from spinta.utils.path import is_ignored
from spinta.config import RawConfig
from spinta import exceptions
from spinta import commands
from spinta.migrations import (
    get_new_schema_version,
    get_parents,
    get_schema_changes,
)

log = logging.getLogger(__name__)

yaml = YAML(typ='safe')


class YamlManifest(Manifest):
    pass


@commands.load.register()
def load(context: Context, manifest: YamlManifest, rc: RawConfig):
    config = context.get('config')

    # Add all supported node types.
    manifest.objects = {}
    for name in config.components['nodes'].keys():
        manifest.objects[name] = {}

    manifest.parent = None
    manifest.endpoints = {}

    manifest.path = rc.get('manifests', manifest.name, 'path', cast=pathlib.Path, required=True)
    log.info('Loading manifest %r from %s.', manifest.name, manifest.path.resolve())

    for node, data, versions in iter_nodes(context, manifest):
        load(context, node, data, manifest)
        manifest.objects[node.type][node.name] = node

    return manifest


@commands.freeze.register()
def freeze(context: Context, manifest: YamlManifest):
    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.width = 80
    yaml.explicit_start = False

    # load all model yamls into memory as cache to avoid multiple file reads
    config = context.get('config')
    commands.load(context, manifest, config.raw)

    manifest.objects = {}
    manifest.freezed = {}  # Already freezed nodes.
    for name in config.components['nodes'].keys():
        manifest.objects[name] = {}
        manifest.freezed[name] = {}

    # Load all models and previous version of each model.
    for node, data, versions in iter_nodes(context, manifest):
        load(context, node, data, manifest)
        manifest.objects[node.type][node.name] = node
        manifest.freezed[node.type][node.name] = load_freezed_model(
            context, config, manifest, versions,
        )

    # Freeze changes.
    for node_type, nodes in manifest.objects.items():
        for node in nodes.values():
            prev = manifest.freezed[node_type][node.name]
            changes = get_schema_changes(prev, node)
            if changes:
                version = get_new_schema_version(changes, actions, parents)
                freeze(context, node.backend, node, prev=prev)

                version = {
                    'version': {
                        'id': version.id,
                        'date': version.date,
                        'parents': version.parents,
                    },
                    'changes': changes,
                    'migrate': {
                        'schema': actions,
                    },
                }

                vnum = version['version']['id']
                print(f"Updating to version {vnum}: {node.path}")
                versions[0]['version'] = version['version']
                versions.append(version)
                with node.path.open('w') as f:
                    yaml.dump_all(versions, f)


def iter_nodes(context: Context, manifest: Manifest):
    config = context.get('config')
    ignore = config.raw.get('ignore', default=[], cast=list)

    for file in manifest.path.glob('**/*.yml'):
        if is_ignored(ignore, manifest.path, file):
            continue

        versions = yaml.load_all(file.read_text())

        try:
            data = next(versions)
        except (ParserError, ScannerError, YAMLError) as e:
            raise exceptions.InvalidManifestFile(
                manifest=manifest.name,
                filename=file,
                error=str(e),
            )
        data = {
            'path': file,
            'parent': manifest,
            'backend': manifest.backend,
            **data,
        }
        node = init_node(config, manifest, data)
        return node, data, versions


def init_node(config: Config, manifest: Manifest, data: dict):
    if not isinstance(data, dict):
        raise exceptions.InvalidManifestFile(
            manifest=manifest.name,
            filename=data['path'],
            error=f"Expected dict got {data.__class__.__name__}.",
        )

    if 'type' not in data:
        raise exceptions.InvalidManifestFile(
            manifest=manifest.name,
            filename=data['path'],
            error=f"Required parameter 'type' is not defined.",
        )

    if data['type'] not in manifest.objects:
        raise exceptions.InvalidManifestFile(
            manifest=manifest.name,
            filename=data['path'],
            error=f"Unknown type {data['type']!r}.",
        )

    if data['name'] in manifest.objects[data['type']]:
        raise exceptions.InvalidManifestFile(
            manifest=manifest.name,
            filename=data['path'],
            error=(
                f"Node {data['type']} with name {data['name']} already "
                f"defined in {manifest.objects[data['type']].path}."
            ),
        )

    Node = config.components['nodes'][data['type']]
    return Node()


def load_freezed_model(
    context: Context,
    config: Config,
    manifest: Manifest,
    versions: Iterable[dict],
):
    data = {}
    for version in versions:
        patch = version.get('changes')
        if patch:
            patch = jsonpatch.JsonPatch(patch)
            data = patch.apply(data)
    if data:
        node = init_node(config, manifest, data)
        return commands.load(context, node, data, manifest)
