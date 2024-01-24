from spinta import commands
from spinta.components import Context
from spinta.manifests.components import Manifest
from spinta.types.datatype import DataType
from spinta.types.text.components import Text


@commands.load.register(Context, Text, dict, Manifest)
def load(context: Context, dtype: Text, data: dict, manifest: Manifest) -> DataType:
    props = {}
    for name, params in (dtype.langs or {}).items():
        place = dtype.prop.place + '@' + name
        prop = dtype.prop.__class__()
        prop.name = name
        prop.place = place
        prop.parent = dtype.prop
        prop.model = dtype.prop.model
        prop.list = dtype.prop.list
        commands.load(context, prop, params, manifest)
        if prop.access is None:
            prop.access = dtype.prop.access
        dtype.prop.model.flatprops[place] = prop
        props[name] = prop
    dtype.langs = props
    return dtype
