from spinta import commands
from spinta.components import Context, Node
from spinta.manifests.components import Manifest


@commands.link.register(Context, Manifest)
def link(context: Context, manifest: Manifest):
    manifest.backend = manifest.store.backends[manifest.backend]
    for nodes in manifest.objects.values():
        for node in nodes.values():
            commands.link(context, node)


@commands.link.register(Context, Node)
def link(context: Context, node: Node):
    pass
