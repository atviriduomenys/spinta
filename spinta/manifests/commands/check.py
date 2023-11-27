from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest, get_manifest_object_names


@commands.check.register(Context, Manifest)
def check(context: Context, manifest: Manifest):
    for node in get_manifest_object_names():
        for obj in commands.get_nodes(context, manifest, node).values():
            check(context, obj)
