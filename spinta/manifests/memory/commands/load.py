from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.manifests.helpers import load_manifest_nodes
from spinta.manifests.memory.components import MemoryManifest


@commands.load.register(Context, MemoryManifest)
def load(
     context: Context,
     manifest: MemoryManifest,
     *,
     into: Manifest = None,
     freezed: bool = True,
     rename_duplicates: bool = False,
     load_internal: bool = True,
):
    if load_internal:
        target = into or manifest
        if not commands.has_model(target, '_schema'):
            store = context.get('store')
            commands.load(context, store.internal, into=target)

    for source in manifest.sync:
        commands.load(
            context, source,
            into=into or manifest,
            freezed=freezed,
            rename_duplicates=rename_duplicates,
            load_internal=load_internal,
        )
