from __future__ import annotations

from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.open_api.components import OpenAPIManifest
from spinta.manifests.open_api.helpers import read_open_api_manifest


@commands.load.register(Context, OpenAPIManifest)
def load(
    context: Context,
    manifest: OpenAPIManifest,
    *,
    into: Manifest | None = None,
    freezed: bool = False,
    rename_duplicates: bool = False,
    load_internal: bool = True,
    full_load: bool = False,
) -> None:
    if load_internal:
        target = into or manifest
        if not commands.has_model(context, target, "_schema"):
            store = context.get("store")
            commands.load(context, store.internal, into=target, full_load=full_load)

    if (path := manifest.path) is None:
        return

    path = path.split("://")[-1]  # FIXME: Temporary, because OpenAPI files are prefixed with 'openapi+file://'.
    schemas = read_open_api_manifest(path)

    if into:
        load_manifest_nodes(context, into, schemas, source=manifest)
    else:
        load_manifest_nodes(context, manifest, schemas)

    for source in manifest.sync:
        commands.load(
            context,
            source,
            into=into or manifest,
            freezed=freezed,
            rename_duplicates=rename_duplicates,
            load_internal=load_internal,
        )
