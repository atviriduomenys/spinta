from spinta import commands
from spinta.components import Context, MetaData
from spinta.dimensions.param.helpers import finalize_param_link
from spinta.dimensions.scope.helpers import finalize_scope_link
from spinta.manifests.components import Manifest, get_manifest_object_names
from spinta.types.namespace import build_namespaces


@commands.link.register(Context, Manifest)
def link(context: Context, manifest: Manifest):
    # Collect namespaces from datasets and models before linking, so the linking
    # can rely on the whole namespace tree being present (see #1271).
    build_namespaces(context, manifest)

    for node in get_manifest_object_names():
        for obj in commands.get_nodes(context, manifest, node).values():
            commands.link(context, obj)

    finalize_param_link(context, manifest)
    finalize_scope_link(context, manifest)


@commands.link.register(Context, MetaData)
def link(context: Context, node: MetaData):
    pass
