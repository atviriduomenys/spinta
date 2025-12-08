from spinta import commands
from spinta.components import Context
from spinta.core.config import RawConfig
from spinta.exceptions import ModelNotFound
from spinta.manifests.components import Manifest, get_manifest_object_names


@commands.check.register(Context, Manifest)
def check(context: Context, manifest: Manifest):
    for node in get_manifest_object_names():
        for obj in commands.get_nodes(context, manifest, node).values():
            check(context, obj)

    # Check if the configuration does not contain unknown models
    rc: RawConfig = context.get("rc")
    if rc.has("models"):
        for name in rc.keys("models"):
            if not commands.has_model(context, manifest, name):
                raise ModelNotFound(manifest, model=name)
