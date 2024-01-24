from spinta import commands
from spinta.components import Context, MetaData
from spinta.manifests.components import Manifest, get_manifest_object_names


@commands.link.register(Context, Manifest)
def link(context: Context, manifest: Manifest):
    for node in get_manifest_object_names():
        for obj in commands.get_nodes(context, manifest, node).values():
            commands.link(context, obj)


@commands.link.register(Context, MetaData)
def link(context: Context, node: MetaData):
    pass
