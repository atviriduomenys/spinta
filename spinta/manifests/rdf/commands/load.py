from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.rdf.components import RdfManifest
from spinta.manifests.rdf.helpers import read_rdf_manifest


@commands.load.register(Context, RdfManifest)
def load(
    context: Context,
    manifest: RdfManifest,
    *,
    into: Manifest = None,
    freezed: bool = True,
    rename_duplicates: bool = False,
    load_internal: bool = True,
    full_load=False
):
    if load_internal:
        target = into or manifest
        if not commands.has_model(context, target, '_schema'):
            store = context.get('store')
            commands.load(context, store.internal, into=target, full_load=full_load)

    if manifest.path is None:
        return

    dataset_name = context.get("rc").get("given_dataset_name")
    schemas = read_rdf_manifest(manifest, dataset_name)

    if into:
        load_manifest_nodes(context, into, schemas, source=manifest)
    else:
        load_manifest_nodes(context, manifest, schemas)

    for source in manifest.sync:
        commands.load(
            context, source,
            into=into or manifest,
            freezed=freezed,
            rename_duplicates=rename_duplicates,
            load_internal=load_internal,
            full_load=full_load
        )
