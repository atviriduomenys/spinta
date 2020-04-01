from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest


@commands.check.register(Context, Manifest)
def check(context: Context, manifest: Manifest):
    for objects in manifest.objects.values():
        for obj in objects.values():
            check(context, obj)

    # Check endpoints.
    names = set(manifest.models)
    for model in manifest.models.values():
        if model.endpoint == model.name or model.endpoint in names:
            raise Exception(
                f"Endpoint name can't overshadow existing model names and "
                f"{model.endpoint!r} is already a model name."
            )
