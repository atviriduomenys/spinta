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
from spinta import spyna
from spinta.migrations import (
    SchemaVersion,
    get_new_schema_version,
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

    for file, data, versions in iter_yaml_files(context, manifest):
        data = {
            'path': file,
            'parent': manifest,
            'backend': manifest.backend,
            **data,
        }
        node = init_node(config, manifest, data)
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

    # Load all models, previous version and changes between current and previous
    # version.
    changes = []
    for file, new_data, versions in iter_yaml_files(context, manifest):
        new_data_ = {
            'path': file,
            'parent': manifest,
            'backend': manifest.backend,
            **new_data,
        }
        new = init_node(config, manifest, new_data_)
        load(context, new, new_data_, manifest)
        manifest.objects[new.type][new.name] = new

        old = None
        old_data = load_freezed_model_data(context, config, manifest, versions)
        patch = list(jsonpatch.make_patch(old_data, new_data))
        if patch:
            changes.append((new, patch))
            if old_data:
                old = init_node(config, manifest, old_data)
                old = commands.load(context, old, old_data, manifest)

        manifest.freezed[new.type][new.name] = old

    # Freeze changes.
    for new, patch in changes:
        old = manifest.freezed[new.type][new.name]
        version = get_new_schema_version(patch)
        freeze(context, version, new.backend, old, new)

        print(f"Updating to version {version.id}: {new.path}")
        update_yaml_file(new.path, version)


def iter_yaml_files(context: Context, manifest: Manifest):
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
        yield file, data, versions


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


def load_freezed_model_data(
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
    return data


def update_yaml_file(file: pathlib.Path, version: SchemaVersion):
    versions = yaml.load_all(file.read_text())
    versions = list(versions)
    versions[0]['version']['id'] = version.id
    versions[0]['version']['date'] = version.date
    versions.append({
        'version': {
            'id': version.id,
            'date': version.date,
            'parents': version.parents,
        },
        'changes': version.changes,
        'migrate': [
            {
                'type': 'schema',
                'upgrade': spyna.unparse(action['upgrade'], pretty=True),
                'downgrade': spyna.unparse(action['downgrade'], pretty=True),
            }
            for action in version.actions
        ],
    })
    with file.open('w') as f:
        yaml.dump_all(versions, f)
