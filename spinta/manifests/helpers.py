from spinta import commands
from spinta.nodes import get_node
from spinta.components import Context, Config, Manifest


def load_manifest_node(
    context: Context,
    config: Config,
    manifest: Manifest,
    data: dict,
    *,
    # XXX: This is a temporary workaround and should be removed.
    #      `check is used to not check if node is already defined, we disable
    #      this check for freezed nodes.
    check=True,
):
    node = get_node(config, manifest, data, check=check)
    node.type = data['type']
    node.parent = manifest
    node.manifest = manifest
    return commands.load(context, node, data, manifest)
