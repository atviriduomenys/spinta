from __future__ import annotations

from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.wsdl.components import WsdlManifest
from spinta.manifests.wsdl.helpers import read_schema


@commands.load.register(Context, WsdlManifest)
def load(
    context: Context,
    manifest: WsdlManifest,
    *,
    into: Manifest | None = None,
    freezed: bool = True,
    rename_duplicates: bool = False,
    load_internal: bool = True,
    full_load: bool = False,
) -> None:
    if load_internal:
        target = into or manifest
        if not commands.has_model(context, target, "_schema"):
            store = context.get("store")
            commands.load(context, store.internal, into=target, full_load=full_load)

    if manifest.path is None:
        return

    dataset_name = context.get("rc").get("given_dataset_name")
    schemas = read_schema(context, manifest, manifest.path, dataset_name)

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
            full_load=full_load,
        )
