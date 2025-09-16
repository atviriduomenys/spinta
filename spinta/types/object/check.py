from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Object


@commands.check.register(Context, Object)
def check(context: Context, dtype: Object):
    for prop in dtype.properties.values():
        commands.check(context, prop)
