from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest


@commands.load_for_request.register(Context, Manifest)
def load_for_request(context: Context, manifest: Manifest):
    pass


@commands.initialize_missing_models.register(Context, Manifest)
def initialize_missing_models(context: Context, manifest: Manifest):
    pass


