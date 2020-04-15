from typing import Iterator

import pathlib

from spinta import commands
from spinta.components import Context
from spinta.manifests.backend.components import BackendManifest
from spinta.manifests.backend.helpers import list_schemas
from spinta.manifests.backend.helpers import read_schema
from spinta.manifests.backend.helpers import get_last_version_eid
from spinta.manifests.backend.helpers import get_version_schema


@commands.manifest_list_schemas.register(Context, BackendManifest)
def manifest_list_schemas(
    context: Context,
    manifest: BackendManifest,
) -> Iterator[str]:
    yield from list_schemas(context, manifest)


@commands.manifest_read_current.register(Context, BackendManifest)
def manifest_read_current(
    context: Context,
    manifest: BackendManifest,
    *,
    eid: str,
) -> dict:
    return read_schema(context, manifest, eid)


@commands.manifest_read_freezed.register(Context, BackendManifest)
def manifest_read_freezed(
    context: Context,
    manifest: BackendManifest,
    *,
    eid: str,
) -> dict:
    version_eid = get_last_version_eid(context, manifest, eid)
    return get_version_schema(context, manifest, version_eid)


@commands.manifest_read_versions.register(Context, BackendManifest)
def manifest_read_versions(
    context: Context,
    manifest: BackendManifest,
    *,
    eid: pathlib.Path,
) -> Iterator[dict]:
    raise NotImplementedError
