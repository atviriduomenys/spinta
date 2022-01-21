from __future__ import annotations

from typing import TYPE_CHECKING, Iterator, Optional, Tuple, List

import pathlib
import uuid

import jsonpatch

from ruamel.yaml import YAML
from ruamel.yaml.parser import ParserError
from ruamel.yaml.scanner import ScannerError
from ruamel.yaml.error import YAMLError
from ruamel.yaml.scalarstring import walk_tree

from spinta import spyna
from spinta import exceptions
from spinta.exceptions import InvalidManifestFile
from spinta.manifests.yaml.components import InlineManifest
from spinta.utils.itertools import last
from spinta.utils.path import is_ignored
from spinta.components import Context

if TYPE_CHECKING:
    from spinta.migrations import SchemaVersion
    from spinta.manifests.yaml.components import YamlManifest

yaml = YAML(typ='safe')


def yaml_config_params(context: Context, manifest: YamlManifest) -> None:
    # We do not load manifest.path here, because internal manifest loads it
    # differently. Here we only load commong config parameters.
    rc = context.get('rc')
    manifest.ignore = rc.get('ignore', default=[], cast=list)
    manifest.ignore += rc.get('manifests', manifest.name, 'ignore', default=[], cast=list)


def list_yaml_files(manifest: YamlManifest) -> Iterator[pathlib.Path]:
    for file in manifest.path.glob('**/*.yml'):
        if not is_ignored(manifest.ignore, manifest.path, file):
            yield file


def read_yaml_file(path: pathlib.Path):
    try:
        with path.open() as f:
            for data in yaml.load_all(f):
                yield data
    except (ParserError, ScannerError, YAMLError) as e:
        raise exceptions.InvalidManifestFile(
            filename=path,
            error=str(e),
        )


def read_schema_versions(path: pathlib.Path):
    docs = read_yaml_file(path)
    schema = {}
    current = next(docs)
    for version in docs:
        patch = version.get('changes', [])
        patch = jsonpatch.JsonPatch(patch)
        schema = patch.apply(schema)
        if 'id' in current:
            schema['id'] = current['id']
        elif 'id' in schema:
            del schema['id']
        if 'id' not in version:
            raise InvalidManifestFile(
                eid=path,
                error="Version id is not specified.",
            )
        schema['version'] = version['id']
        yield {
            **version,
            'schema': schema,
            'migrate': [
                {
                    **action,
                    'upgrade': spyna.parse(action['upgrade']),
                    'downgrade': spyna.parse(action['downgrade']),
                }
                for action in version['migrate']
            ],
        }


def read_manifest_schemas(
    manifest: YamlManifest,
) -> Iterator[Tuple[pathlib.Path, Optional[dict]]]:
    for path in list_yaml_files(manifest):
        yield path, next(read_yaml_file(path), None)


def read_freezed_manifest_schemas(
    manifest: YamlManifest,
) -> Iterator[Tuple[pathlib.Path, Optional[dict], List[dict]]]:
    for path in list_yaml_files(manifest):
        freezed = last(read_schema_versions(path), None)
        if freezed:
            yield path, freezed['schema']


def read_inline_manifest_schemas(
    manifest: InlineManifest,
) -> Iterator[Tuple[None, Optional[dict]]]:
    for schema in manifest.manifest:
        yield None, schema


def add_new_version(path: pathlib.Path, version: SchemaVersion):

    # Read YAML file
    with path.open() as f:
        versions = yaml.load_all(f)
        versions = list(versions)

    # Update current version
    current = versions[0]

    # Ensure schema id
    if 'id' not in current:
        current['id'] = str(uuid.uuid4())

    # Update current version
    current['version'] = version.id

    # Add new version
    versions.append({
        'id': version.id,
        'date': version.date,
        'parents': version.parents,
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

    yml = YAML()
    yml.indent(mapping=2, sequence=4, offset=2)
    yml.width = 80
    yml.explicit_start = False

    # Use LiteralScalarString format for all multiline strings.
    walk_tree(versions)

    # Write updates back to YAML file
    with path.open('w') as f:
        yml.dump_all(versions, f)
