from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest


@commands.inspect.register(Context, Manifest)
def inspect(context: Context, manifest: Manifest):
    for dataset in manifest.objects['dataset'].values():
        commands.inspect(context, dataset)
