from spinta import commands
from spinta.components import Context, MetaData
from spinta.manifests.components import Manifest


@commands.link.register(Context, Manifest)
def link(context: Context, manifest: Manifest):
    for nodes in manifest.objects.values():
        for node in nodes.values():
            commands.link(context, node)


@commands.link.register(Context, MetaData)
def link(context: Context, node: MetaData):
    pass
