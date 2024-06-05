from spinta import commands
from spinta.backends import Backend
from spinta.components import Context
from spinta.ufuncs.resultbuilder.components import ResultBuilder


@commands.get_result_builder.register(Context, Backend)
def get_result_builder(
    context: Context,
    backend: Backend
):
    return ResultBuilder(context)
