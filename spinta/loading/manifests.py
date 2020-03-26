from spinta import commands


@commands.load.register()
def load(context: Context, manifest: Manifest) -> None:
    manifest = model.parent
    _add_model_endpoint(model)
    model.ns = _load_model_namespace(context, model)
    model.external = _load_model_external_source(context, manifest, model)
    model.properties = _load_model_properties(context, model)
