from spinta import commands
from spinta.components import Context
from spinta.types.datatype import Array


@commands.check.register(Context, Array)
def check(context: Context, dtype: Array):
    commands.check(context, dtype.items)
