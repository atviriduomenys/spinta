from typing import Iterable

import logging
import pathlib

import jsonpatch

from ruamel.yaml import YAML
from ruamel.yaml.parser import ParserError
from ruamel.yaml.scanner import ScannerError
from ruamel.yaml.error import YAMLError

from spinta.components import Context, Config, Manifest, Model
from spinta.utils.path import is_ignored
from spinta.core.config import RawConfig
from spinta import exceptions
from spinta import commands
from spinta import spyna
from spinta.nodes import get_node
from spinta.migrations import (
    SchemaVersion,
    get_new_schema_version,
)

log = logging.getLogger(__name__)

yaml = YAML(typ='safe')


class YamlManifest(Manifest):

    def load(self, config):
        self.path = config.rc.get(
            'manifests', self.name, 'path',
            cast=pathlib.Path,
            required=True,
        )

    def read(self, context: Context):
        for file, data, versions in iter_yaml_files(context, self):
            data['path'] = file
            versions = list(versions)
            for v in versions:
                assert 'function' not in v['version']['id'], file
            yield data, versions


@commands.load.register()
def load(context: Context, manifest: YamlManifest, rc: RawConfig):
    config = context.get('config')
    manifest.load(config)
    log.info('Loading manifest %r from %s.', manifest.name, manifest.path.resolve())
    for data, versions in manifest.read(context):
        node = _load(context, config, manifest, data)
        manifest.objects[node.type][node.name] = node
    return manifest


@commands.freeze.register()
def freeze(context: Context, manifest: YamlManifest):
    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.width = 80
    yaml.explicit_start = False

    manifest.objects = {}
    manifest.freezed = {}  # Already freezed nodes.
    config = context.get('config')
    for name in config.components['nodes'].keys():
        manifest.objects[name] = {}
        manifest.freezed[name] = {}

    changes = []
    for data, versions in manifest.read(context):
        # Load current model version
        current = _load(context, config, manifest, data)
        manifest.objects[current.type][current.name] = current

        # Load freezed model version
        patch, freezed = _load_freezed(context, config, manifest, data, versions)
        if patch:
            changes.append((current, patch))
        manifest.freezed[current.type][current.name] = freezed

    # Freeze changes.
    for current, patch in changes:
        freezed = manifest.freezed[current.type][current.name]
        version = get_new_schema_version(patch)

        if isinstance(current, Model):
            freeze(context, version, current.backend, freezed, current)

        print(f"Add new version {version.id} to {current.path}")
        update_yaml_file(current.path, version)


def _load_freezed(
    context: Context,
    config: Config,
    manifest: Manifest,
    data: dict,
    versions: Iterable[dict],
):
    freezed_data = _get_freezed_schema(versions)
    data = data.copy()
    path = data.pop('path')
    patch = list(jsonpatch.make_patch(freezed_data, data))
    if patch and freezed_data:
        freezed = _load(context, config, manifest, {
            **freezed_data,
            'path': path,
        }, check=False)
        return patch, freezed
    return patch, None


def _get_freezed_schema(versions: Iterable[dict]):
    data = {}
    for version in versions:
        patch = version.get('changes')
        patch = jsonpatch.JsonPatch(patch)
        data = patch.apply(data)
    return data


def _load(
    context: Context,
    config: Config,
    manifest: Manifest,
    data: dict,
    *,
    # XXX: This is a temporary workaround and should be removed.
    #      `check is used to not check if node is already defined, we disable
    #      this check for freezed nodes.
    check=True,
):
    data = {
        **data,
        'parent': manifest,
    }
    node = get_node(config, manifest, data, check)
    return load(context, node, data, manifest)


def iter_yaml_files(context: Context, manifest: Manifest):
    config = context.get('config')
    ignore = config.rc.get('ignore', default=[], cast=list)

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


def update_yaml_file(file: pathlib.Path, version: SchemaVersion):
    # Read YAML file
    versions = yaml.load_all(file.read_text())
    versions = list(versions)

    # Update current version
    current = versions[0]
    if 'version' not in current:
        current['version'] = {}
    current['version']['id'] = version.id
    current['version']['date'] = version.date

    # Add new version
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

    # Write updates back to YAML file
    with file.open('w') as f:
        yaml.dump_all(versions, f)


@commands.bootstrap.register()
def bootstrap(context: Context, manifest: YamlManifest):
    # Yaml manifest can't store state so we always run bootstrap.
    store = context.get('store')
    for backend in store.backends.values():
        commands.bootstrap(context, backend)


@commands.migrate.register()
def migrate(context: Context, manifest: YamlManifest):
    raise Exception(
        "Can't run migrations on 'yaml' manifest, use `spinta bootstrap` "
        "command instead."
    )


@commands.sync.register()
def sync(context: Context, manifest: YamlManifest):
    if manifest.sync:
        # TODO: sync YAML files from other manifests
        raise NotImplementedError
