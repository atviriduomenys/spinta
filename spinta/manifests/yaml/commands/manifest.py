from typing import Iterator

import pathlib

from spinta import commands
from spinta.utils.itertools import last
from spinta.components import Context
from spinta.manifests.yaml.components import YamlManifest
from spinta.manifests.yaml.helpers import list_yaml_files
from spinta.manifests.yaml.helpers import read_yaml_file
from spinta.manifests.yaml.helpers import read_schema_versions


@commands.manifest_list_schemas.register(Context, YamlManifest)
def manifest_list_schemas(
    context: Context,
    manifest: YamlManifest,
) -> Iterator[pathlib.Path]:
    yield from list_yaml_files(manifest)


@commands.manifest_read_current.register(Context, YamlManifest)
def manifest_read_current(
    context: Context,
    manifest: YamlManifest,
    *,
    eid: pathlib.Path,
) -> dict:
    return next(read_yaml_file(eid), None)


@commands.manifest_read_freezed.register(Context, YamlManifest)
def manifest_read_freezed(
    context: Context,
    manifest: YamlManifest,
    *,
    eid: pathlib.Path,
) -> dict:
    return last(read_schema_versions(eid), default={'schema': None})['schema']


@commands.manifest_read_versions.register(Context, YamlManifest)
def manifest_read_versions(
    context: Context,
    manifest: YamlManifest,
    *,
    eid: pathlib.Path,
) -> Iterator[dict]:
    yield from read_schema_versions(eid)
