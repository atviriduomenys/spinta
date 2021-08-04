from spinta import commands
from spinta.migrations import get_new_schema_version
from spinta.components import Context
from spinta.manifests.yaml.components import YamlManifest
from spinta.manifests.helpers import create_manifest
from spinta.manifests.helpers import get_current_schema_changes
from spinta.manifests.yaml.helpers import add_new_version


@commands.freeze.register(Context, YamlManifest)
def freeze(context: Context, current: YamlManifest):
    # Load current node, that is not yet freezed.
    commands.load(context, current, freezed=False)
    commands.link(context, current)
    commands.check(context, current)

    # Load previously freezed version, we don't need to run check, because
    # freezed version was already checked before freezing it.
    freezed = create_manifest(context, current.store, current.name)
    commands.load(context, freezed, freezed=True)
    commands.link(context, freezed)

    # Read current nodes, get freezed versions and freeze changes between
    # current and previously freezed node into a new version.
    for ntype, nodes in current.objects.items():

        if ntype == 'ns':
            # Namespaces do not have physical form, yet, se there is no need to
            # freeze them.
            continue

        for name, cnode in nodes.items():

            # Get freezed node
            if name in freezed.objects[ntype]:
                fnode = freezed.objects[ntype][name]
            else:
                fnode = None

            # Check if current and freezed schema versions changed
            changes = get_current_schema_changes(context, current, cnode.eid)
            if not changes:
                continue

            # Autodectect and write changes into version instance.
            version = get_new_schema_version(changes)
            commands.freeze(context, version, cnode.backend, fnode, cnode)

            # Write version data back to node file.
            print(f"Adding new version {version.id} to {cnode.eid}")
            add_new_version(cnode.eid, version)
