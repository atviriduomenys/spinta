from spinta import commands
from spinta.components import Context
from spinta.manifests.internal.components import InternalManifest


@commands.freeze.register()
def freeze(context: Context, manifest: InternalManifest):
    # You can't freeze internal manifest directly, because internal manifest
    # schema can only be updated after migrations are applied.

    # But other manifests from which this manifests is synced, can be updated.
    for source in manifest.sync:
        commands.freeze(context, source)
