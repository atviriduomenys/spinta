from typing import Iterable

import pathlib

import jsonpatch

from ruamel.yaml import YAML

from spinta import spyna
from spinta import commands
from spinta.components import Context, Config, Manifest, Model
from spinta.manifests.yaml.components import YamlManifest
from spinta.manifests.helpers import load_manifest_node
from spinta.migrations import SchemaVersion, get_new_schema_version

yaml = YAML(typ='safe')


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
        current = load_manifest_node(context, config, manifest, data)
        manifest.objects[current.type][current.name] = current
        commands.link(context, current)

        # Load freezed model version
        patch, freezed = _load_freezed(context, config, manifest, data, versions)
        if patch:
            changes.append((current, patch))
        if freezed is not None:
            commands.link(context, freezed)
        manifest.freezed[current.type][current.name] = freezed

    # Freeze changes.
    for current, patch in changes:
        freezed = manifest.freezed[current.type][current.name]
        version = get_new_schema_version(patch)

        if isinstance(current, Model):
            freeze(context, version, current.backend, freezed, current)

        print(f"Add new version {version.id} to {current.path}")
        update_yaml_file(current.path, version)


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
        freezed = load_manifest_node(context, config, manifest, {
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
